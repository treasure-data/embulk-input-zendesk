package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.models.AuthenticationMethod;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.utils.JacksonTimestampValueLocator;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ZendeskInputPluginDelegate implements RestClientInputPluginDelegate<ZendeskInputPluginDelegate.PluginTask>
{
    public interface PluginTask extends RestClientInputTaskBase
    {
        @Config("login_url")
        String getLoginUrl();

        @Config("auth_method")
        @ConfigDefault("basic")
        AuthenticationMethod getAuthenticationMethod();

        @Config("target")
        Target getTarget();

        @Config("username")
        @ConfigDefault("null")
        Optional<String> getUsername();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("token")
        @ConfigDefault("null")
        Optional<String> getToken();

        @Config("access_token")
        @ConfigDefault("null")
        Optional<String> getAccessToken();

        @Config("start_time")
        @ConfigDefault("null")
        Optional<String> getStartTime();

        @Min(1)
        @Max(30)
        @Config("retry_limit")
        @ConfigDefault("5")
        int getRetryLimit();

        @Min(1)
        @Max(3600)
        @Config("retry_initial_wait_sec")
        @ConfigDefault("5")
        int getRetryInitialWaitSec();

        @Min(30)
        @Max(300)
        @Config("max_retry_wait_sec")
        @ConfigDefault("60")
        int getMaxRetryWaitSec();

        @Min(3)
        @Max(100)
        @Config("connection_timeout")
        @ConfigDefault("30")
        int getConnectionTimeout();

        @Config("incremental")
        @ConfigDefault("true")
        boolean getIncremental();

        @Config("includes")
        @ConfigDefault("[]")
        List<String> getIncludes();

        @Config("dedup")
        @ConfigDefault("true")
        boolean getDedup();

        @Config("app_marketplace_integration_name")
        @ConfigDefault("null")
        Optional<String> getAppMarketPlaceIntegrationName();

        @Config("app_marketplace_org_id")
        @ConfigDefault("null")
        Optional<String> getAppMarketPlaceOrgId();

        @Config("app_marketplace_app_id")
        @ConfigDefault("null")
        Optional<String> getAppMarketPlaceAppId();

        @Config("previous_records")
        @ConfigDefault("[]")
        List<String> getPreviousRecords();

        void setStartTime(String startTime);

        Schema getSchema();

        void setSchema(Schema schema);
    }
    private static final Logger logger = Exec.getLogger(ZendeskInputPluginDelegate.class);

    private ZendeskSupportAPIService zendeskSupportAPIService;

    @Override
    public ConfigDiff buildConfigDiff(final PluginTask task, final Schema schema, final int taskCount,
                                      final List<TaskReport> taskReports)
    {
        final ConfigDiff configDiff = Exec.newConfigDiff();

        if (!taskReports.isEmpty()) {
            final TaskReport taskReport = taskReports.get(0);

            if (task.getIncremental()) {
                if (taskReport.has(ZendeskConstants.Field.START_TIME)) {
                    OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(
                            taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong()), ZoneOffset.UTC);

                    configDiff.set(ZendeskConstants.Field.START_TIME,
                            offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT)));
                }
                if (taskReport.has(ZendeskConstants.Field.PREVIOUS_RECORDS)) {
                    configDiff.set(ZendeskConstants.Field.PREVIOUS_RECORDS,
                            taskReport.get(JsonNode.class, ZendeskConstants.Field.PREVIOUS_RECORDS));
                }
            }
        }
        return configDiff;
    }

    @Override
    public void validateInputTask(final PluginTask task)
    {
        validateHost(task.getLoginUrl());
        validateAppMarketPlace(task.getAppMarketPlaceIntegrationName().isPresent(), task.getAppMarketPlaceAppId().isPresent(),
                task.getAppMarketPlaceOrgId().isPresent());
        validateCredentials(task);
        validateInclude(task.getIncludes(), task.getTarget());
        validateIncremental(task);
    }

    private void validateHost(final String loginUrl)
    {
        Matcher matcher = Pattern.compile(ZendeskConstants.Regex.HOST).matcher(loginUrl);
        if (!matcher.matches()) {
            throw new ConfigException(String.format("Login URL, '%s', is unmatched expectation. " +
                    "It should be followed this format: https://abc.zendesk.com/", loginUrl));
        }
    }

    private void validateInclude(final List<String> includes, final Target target)
    {
        if (includes != null && !includes.isEmpty()) {
            if (!ZendeskUtils.isSupportInclude(target)) {
                logger.warn("Target: '{}' doesn't support include size loading. Option include will be ignored", target.toString());
            }
        }
    }

    private void validateCredentials(final PluginTask task)
    {
        switch (task.getAuthenticationMethod()) {
            case OAUTH:
                if (!task.getAccessToken().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for authenticated by '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case TOKEN:
                if (!task.getUsername().isPresent() || !task.getToken().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for authenticated by '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case BASIC:
                if (!task.getUsername().isPresent() || !task.getPassword().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for authenticated by '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            default:
                throw new ConfigException("Unknown authentication method");
        }

        // Validate credentials by sending one request to users.json. It Should always have at least one user
        try {
            getZendeskSupportAPIService(task).getData(String.format("%s/%s/users.json?per_page=1", task.getLoginUrl(),
                    ZendeskConstants.Url.API), 0, false);
        }
        catch (final HttpResponseException ex) {
            if (ex.getResponse().getStatus() == 401) {
                throw new ConfigException("Invalid credential. Error 401: can't authenticate");
            }
        }
    }

    private void validateAppMarketPlace(final boolean isAppMarketIntegrationNamePresent, final boolean isAppMarketAppIdPresent,
                                        final boolean isAppMarketOrgIdPresent)
    {
        final boolean isAllAvailable = isAppMarketIntegrationNamePresent && isAppMarketAppIdPresent && isAppMarketOrgIdPresent;
        final boolean isAllUnAvailable = !isAppMarketIntegrationNamePresent && !isAppMarketAppIdPresent && !isAppMarketOrgIdPresent;
        // All or nothing needed
        if (!(isAllAvailable || isAllUnAvailable)) {
            throw new ConfigException("All of app_marketplace_integration_name, app_marketplace_org_id, app_marketplace_app_id " +
                    "are required to fill out for Apps Marketplace API header");
        }
    }

    private void validateIncremental(final PluginTask task)
    {
        if (task.getIncremental()) {
            if (task.getDedup()) {
                logger.warn("You've selected to skip de-duplicating records, result may contain duplicated data");
            }
            if (!ZendeskUtils.isSupportIncremental(task.getTarget())) {
                throw new ConfigException(String.format("Unsupported incremental options for target '%s'",
                        task.getTarget().toString()));
            }
            // validate start time format
            if (!task.getStartTime().isPresent()) {
                throw new ConfigException("start_time is required for incremental mode");
            }
            else {
                if (!ZendeskDateUtils.isSupportedTimeFormat(task.getStartTime().get(),
                        ImmutableList.of(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT, ZendeskConstants.Misc.SHORT_DATE_FORMAT))) {
                    // it followed the logic in the old version
                    task.setStartTime(ZendeskConstants.Misc.DEFAULT_START_TIME);
                }
            }
        }
        else {
            if (Target.TICKET_EVENTS.equals(task.getTarget())) {
                throw new ConfigException("Zendesk doesn't support non incremental endpoint for ticket_events. " +
                        "Please rerun this job in incremental mode");
            }
            if (task.getStartTime().isPresent()) {
                logger.warn("Target: {} doesn't support incremental export API. Will be ignored start_time option",
                        task.getTarget().toString());
            }
        }
    }

    @Override
    public TaskReport ingestServiceData(final PluginTask task, final RecordImporter recordImporter, final int taskIndex,
                                        final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();
        // Page start from 1 => page = taskIndex + 1
        final JsonNode result = getZendeskSupportAPIService(task).getData("", taskIndex + 1, false);
        final String targetJsonName = task.getTarget().getJsonName();

        if (!result.has(targetJsonName) || !result.get(targetJsonName).isArray()) {
            throw new DataException(String.format("Missing '%s' from Zendesk API response", targetJsonName));
        }

        final Iterator<JsonNode> iterator = result.get(targetJsonName).elements();

        if (task.getIncremental()) {
            final long endTimeStamp = result.get(ZendeskConstants.Field.END_TIME).asLong();
            final int numberOfRecords = result.get(ZendeskConstants.Field.COUNT).asInt();
            final boolean isNextIncrementalAvailable = numberOfRecords >= ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL;

            final ImmutableList.Builder<String> dedupRecordsBuilder = new ImmutableList.Builder<>();
            importDataForIncremental(iterator, task, isNextIncrementalAvailable, dedupRecordsBuilder, endTimeStamp, recordImporter, pageBuilder);
            storeInformationForNextRun(isNextIncrementalAvailable, endTimeStamp, dedupRecordsBuilder, taskReport);
        }
        else {
            while (iterator.hasNext()) {
                fetchData(iterator.next(), task, recordImporter, pageBuilder);
                // only need to run one hit if in preview mode
                if (Exec.isPreview()) {
                    break;
                }
            }
        }
        return taskReport;
    }

    private void importDataForIncremental(final Iterator<JsonNode> iterator, final PluginTask task, final boolean isNextIncrementalAvailable,
                                          final ImmutableList.Builder<String> dedupRecordsBuilder, final long incrementalEndTimeToEpochSecond,
                                          final RecordImporter recordImporter, final PageBuilder pageBuilder)
    {
        final List<String> previousRecordsList = task.getPreviousRecords();

        boolean isStopDedup = false;

        while (iterator.hasNext()) {
            final JsonNode recordJsonNode = iterator.next();

            // For incremental, records sorted by updated_at.
            // So we just need to store some records, which have the same updated_at time as end_time for de-duplication
            if (task.getDedup()) {
                final String recordId = recordJsonNode.get(ZendeskConstants.Field.ID).asText();
                final String recordUpdatedAtTime = recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText();
                final long recordUpdatedAtToEpochSecond = ZendeskDateUtils.isoToEpochSecond(recordUpdatedAtTime);

                if (isStopDedup) {
                    fetchData(recordJsonNode, task, recordImporter, pageBuilder);
                }
                else {
                    boolean isDuplicatedRecord = previousRecordsList.contains(recordId);
                    if (!isDuplicatedRecord) {
                        fetchData(recordJsonNode, task, recordImporter, pageBuilder);
                    }
                    // validate whether we have to check duplication for next records
                    isStopDedup = isRequiredForFurtherChecking(task.getStartTime(), recordUpdatedAtToEpochSecond);
                }

                addRecordIdForNextRun(isNextIncrementalAvailable, incrementalEndTimeToEpochSecond, recordUpdatedAtToEpochSecond,
                        dedupRecordsBuilder, recordId);
            }
            else {
                fetchData(recordJsonNode, task, recordImporter, pageBuilder);
            }
            // only need to run one hit if in preview mode
            if (Exec.isPreview()) {
                break;
            }
        }
    }

    private void storeInformationForNextRun(final boolean isNextIncrementalAvailable, final long endTimeStamp,
                                            final ImmutableList.Builder<String> dedupRecordsBuilder, final TaskReport taskReport)
    {
        if (isNextIncrementalAvailable) {
            taskReport.set(ZendeskConstants.Field.START_TIME, endTimeStamp);
            taskReport.set(ZendeskConstants.Field.PREVIOUS_RECORDS, dedupRecordsBuilder.build());
        }
    }

    private void addRecordIdForNextRun(final boolean isNextIncrementalAvailable, final long incrementalEndTimeToEpochSecond,
                                         final long recordUpdatedTimeToEpochSecond,
                                         final ImmutableList.Builder<String> dedupRecordsBuilder, final String recordId)
    {
        if (isNextIncrementalAvailable) {
            if (incrementalEndTimeToEpochSecond == recordUpdatedTimeToEpochSecond) {
                dedupRecordsBuilder.add(recordId);
            }
        }
    }

    @Override
    public ServiceDataSplitter<PluginTask> buildServiceDataSplitter(final PluginTask task)
    {
        return new ZendeskServiceDataSplitter(getZendeskSupportAPIService(task));
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(final PluginTask task)
    {
        final JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();

        if (task.getSchema() == null) {
            final JsonNode jsonNode = getZendeskSupportAPIService(task).getData("", 0, true);

            final Schema.Builder targetSchemaBuilder = new Schema.Builder();
            final String targetName = task.getTarget().toString();

            if (jsonNode.get(targetName).isArray()) {
                addAllColumnsToSchema(jsonNode, task.getTarget(), task.getIncludes(), targetSchemaBuilder, builder);
            }
            task.setSchema(targetSchemaBuilder.build());
        }
        // when running in non-incremental mode, we run in multiple threads.
        // We just need to build schema one and reuse.
        else  {
            final List<Column> columns = task.getSchema().getColumns();
            for (Column column : columns) {
                if (Types.TIMESTAMP.equals(column.getType())) {
                    builder.add(new JacksonTimestampValueLocator(column.getName()), column.getName(), column.getType(),
                            ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
                }
                else {
                    builder.add(column.getName(), column.getType());
                }
            }
        }

        return builder.build();
    }
    private void addAllColumnsToSchema(final JsonNode jsonNode, final Target target, final List<String> includes,
                                       final Schema.Builder targetSchemaBuilder, final JacksonServiceResponseMapper.Builder builder)
    {
        final Iterator<JsonNode> iterator = jsonNode.get(target.toString()).elements();
        // We always preview with only 1 record by option per_page=1
        if (iterator.hasNext()) {
            final Iterator<Map.Entry<String, JsonNode>> iter = iterator.next().fields();
            while (iter.hasNext()) {
                addSingleColumnToSchema(iter.next(), targetSchemaBuilder, builder);
            }
            addIncludedObjectsToSchema(target, includes, targetSchemaBuilder, builder);
        }
    }

    private void addSingleColumnToSchema(final Map.Entry<String, JsonNode> entry, final Schema.Builder targetSchemaBuilder,
                                         final JacksonServiceResponseMapper.Builder builder)
    {
        final Type columnType = ZendeskUtils.getColumnType(entry.getKey(), entry.getValue());
        targetSchemaBuilder.add(entry.getKey(), columnType);

        if (Types.TIMESTAMP.equals(columnType)) {
            builder.add(new JacksonTimestampValueLocator(entry.getKey()), entry.getKey(), columnType,
                    ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
        }
        else {
            builder.add(entry.getKey(), columnType);
        }
    }

    private void addIncludedObjectsToSchema(final Target target, final List<String> includes, final Schema.Builder targetSchemaBuilder,
                                            final JacksonServiceResponseMapper.Builder builder)
    {
        if (ZendeskUtils.isSupportInclude(target, includes)) {
            for (String include : includes) {
                targetSchemaBuilder.add(include, Types.JSON);
                builder.add(include, Types.JSON);
            }
        }
    }

    private void fetchData(final JsonNode jsonNode, final PluginTask task, final RecordImporter recordImporter,
                           final PageBuilder pageBuilder)
    {
        if (ZendeskUtils.isSupportInclude(task.getTarget(), task.getIncludes())) {
            fetchRelatedObjects(jsonNode, task, recordImporter, pageBuilder);
        }
        else {
            recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) getDataToImport(task.getTarget(), jsonNode)), pageBuilder);
        }
    }

    private void fetchRelatedObjects(final JsonNode jsonNode, final PluginTask task, final RecordImporter recordImporter,
                                     final PageBuilder pageBuilder)
    {
        // remove .json in url
        final String url = jsonNode.get("url").asText().trim().replaceFirst("\\.json", "");

        final StringBuilder builder = new StringBuilder(url);

        Target target = task.getTarget();
        if (ZendeskUtils.isSupportInclude(target)) {
            builder.append("?include=");
            builder.append(task.getIncludes()
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.joining(",")));
        }

        final JsonNode result = getZendeskSupportAPIService(task).getData(builder.toString(), 0, false);

        final JsonNode targetJsonNode = result.get(target.getSingleFieldName());

        for (String include : task.getIncludes()) {
            JsonNode includeJsonNode = result.get(include);
            if (includeJsonNode != null) {
                ((ObjectNode) targetJsonNode).putPOJO(include, includeJsonNode);
            }
        }
        recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) targetJsonNode), pageBuilder);
    }

    private JsonNode getDataToImport(final Target target, final JsonNode jsonNode)
    {
        return Target.TICKET_METRICS.equals(target)
                ? jsonNode.get("metric_set")
                : jsonNode;
    }

    private ZendeskSupportAPIService getZendeskSupportAPIService(final PluginTask task)
    {
        if (zendeskSupportAPIService == null) {
            zendeskSupportAPIService = new ZendeskSupportAPIService(task);
        }
        return zendeskSupportAPIService;
    }

    private boolean isRequiredForFurtherChecking(final Optional<String> startTime, final long recordUpdatedAtTime)
    {
        // Because records sorted by updated_at.
        // When updated_at of one record is larger than start_time, we don't need to check further
        return  startTime.isPresent() && recordUpdatedAtTime > ZendeskDateUtils.isoToEpochSecond(startTime.get());
    }
}
