package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
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
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.clients.ZendeskRestClientImpl;
import org.embulk.input.zendesk.models.AuthenticationMethod;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPiService;
import org.embulk.input.zendesk.services.ZendeskSupportAPiServiceImpl;
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
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ZendeskInputPluginDelegate implements RestClientInputPluginDelegate<ZendeskInputPluginDelegate.PluginTask>
{
    private static final Logger logger = Exec.getLogger(ZendeskInputPluginDelegate.class);

    public interface PluginTask extends RestClientInputTaskBase
    {
        @Config("login_url")
        @ConfigDefault("https://abc.zendesk.com")
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
        @Max(10)
        @Config("retry_limit")
        @ConfigDefault("5")
        int getRetryLimit();

        @Min(1)
        @Max(50)
        @Config("retry_initial_wait_sec")
        @ConfigDefault("2000")
        int getRetryInitialWaitSec();

        @Min(30)
        @Max(300)
        @Config("max_retry_wait_sec")
        @ConfigDefault("60000")
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

        @Config("page")
        @ConfigDefault("0")
        int getPage();

        void setPage(int page);

        Schema getSchema();

        void setSchema(Schema schema);
    }

    private ZendeskSupportAPiService zendeskSupportAPiService;

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

        zendeskSupportAPiService = getZendeskV2SupportAPiService(getZendeskV2RestClient(task), task);

        validateCriteria(task);

        if (task.getIncludes() != null && !task.getIncludes().isEmpty()) {
            validateInclude(task.getTarget());
        }

        if (task.getIncremental()) {
            validateIncremental(task.getStartTime(), task.getTarget());
            if (!task.getDedup()) {
                logger.warn("You've selected to skip de-duplicating records, result may contain duplicated data");
            }
        }
        else {
            if (task.getTarget() == Target.TICKET_EVENTS) {
                throw new ConfigException("Zendesk doesn't support non incremental endpoint for ticket_events. " +
                        "Please rerun this job in incremental mode");
            }
            if (task.getStartTime().isPresent()) {
                logger.warn("Target: {} doesn't support incremental export API. Will be ignored start_time option",
                        task.getTarget().toString());
            }
        }
    }

    private void validateHost(final String loginUrl)
    {
        Matcher matcher = Pattern.compile(ZendeskConstants.Regex.HOST).matcher(loginUrl);
        if (!matcher.matches()) {
            throw new ConfigException(String.format("Login URL, %s,is unmatched expectation. " +
                    "It should be follow this format: https://abc.zendesk.com", loginUrl));
        }
    }

    private void validateInclude(final Target target)
    {
        if (!ZendeskUtils.isSupportInclude(target)) {
            logger.warn("Target: {} doesn't support include size loading. Option include will be ignored ", target.toString());
        }
    }

    private void validateCriteria(final PluginTask task)
    {
        switch (task.getAuthenticationMethod()) {
            case OAUTH:
                if (!task.getAccessToken().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for %s",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case TOKEN:
                if (!task.getUsername().isPresent() || !task.getToken().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for %s",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case BASIC:
                if (!task.getUsername().isPresent() || !task.getPassword().isPresent()) {
                    throw new ConfigException(String.format("Missing required credentials for %s",
                            task.getAuthenticationMethod().name()));
                }
                break;
            default:
                throw new ConfigException("Unknown authentication method");
        }

        // Validate criteria by sending one request to users.json. It Should always have at least one user
        try {
            zendeskSupportAPiService.getData(String.format("%s/%s/users.json?per_page=1", task.getLoginUrl(), ZendeskConstants.Url.API),
                    0, false);
        }
        catch (final HttpResponseException ex) {
            if (ex.getResponse().getStatus() == 401) {
                throw new ConfigException("Invalid credential. Error 401: can't authenticate");
            }
        }
    }

    private void validateAppMarketPlace(boolean isAppMarketIntegrationNamePresent, boolean isAppMarketAppIdPresent,
                                        boolean isAppMarketOrgIdPresent)
    {
        // All or nothing needed
        if (!((isAppMarketIntegrationNamePresent && isAppMarketAppIdPresent && isAppMarketOrgIdPresent)
                || (!isAppMarketIntegrationNamePresent && !isAppMarketAppIdPresent && !isAppMarketOrgIdPresent))) {
            throw new ConfigException("All of app_marketplace_integration_name, app_marketplace_org_id, app_marketplace_app_id " +
                    "are required to fill out for Apps Marketplace API header");
        }
    }

    private void validateIncremental(final Optional<String> startTime, final Target target)
    {
        if (!ZendeskUtils.isSupportIncremental(target)) {
            throw new ConfigException(String.format("Unsupported incremental options for target %s",
                    target.toString()));
        }

        // validate start time format
        if (startTime.isPresent()) {
            ZendeskDateUtils.parse(startTime.get(),
                    ImmutableList.of(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT, ZendeskConstants.Misc.SHORT_DATE_FORMAT));
        }
    }

    @Override
    public TaskReport ingestServiceData(final PluginTask task, final RecordImporter recordImporter, final int taskIndex,
                                        final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();

        // Page start from 1 => page = taskIndex + 1
        final JsonNode result = zendeskSupportAPiService.getData("", taskIndex + 1, false);
        final String targetJsonString = task.getTarget().getJsonName();

        if (!result.has(targetJsonString) || !result.get(targetJsonString).isArray()) {
            throw new DataException(String.format("Missing %s from Zendesk API response", targetJsonString));
        }

        final Iterator<JsonNode> iterator = result.get(targetJsonString).elements();
        final Target target = task.getTarget();

        if (task.getIncremental()) {
            final long endTimeStamp = result.get(ZendeskConstants.Field.END_TIME).asLong();

            // if count >= 1000 -> next incremental available
            final boolean isNextIncrementalAvailable = result.get(ZendeskConstants.Field.COUNT).asInt() >= ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL;

            final List<String> previousRecordsList = task.getPreviousRecords().isEmpty()
                    ? task.getPreviousRecords()
                    : new ArrayList<>();

            boolean isStopDedup = false;

            final ImmutableList.Builder<String> dedupRecordsBuilder = new ImmutableList.Builder<>();

            while (iterator.hasNext()) {
                JsonNode jsonNode = iterator.next();

                // For incremental, sort by updated_date. So just need to store some record have the same updated_at as end_time
                if (task.getDedup()) {
                    long updatedAtTimeStamp = ZendeskDateUtils.toTimeStamp(jsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText());

                    if (isStopDedup) {
                        fetchData(jsonNode, target, task.getIncludes(), recordImporter, pageBuilder);
                    }
                    else {
                        // only check record have the same updated_at timestamp as start_time
                        if (!isDuplicatedRecord(previousRecordsList, jsonNode.get(ZendeskConstants.Field.ID).asText())) {
                            fetchData(jsonNode, target, task.getIncludes(), recordImporter, pageBuilder);
                        }
                        if (task.getStartTime().isPresent()) {
                            isStopDedup = isLaterThanStartTime(task.getStartTime().get(), updatedAtTimeStamp);
                        }
                    }

                    if (isNextIncrementalAvailable) {
                        if (endTimeStamp == updatedAtTimeStamp) {
                            dedupRecordsBuilder.add(jsonNode.get(ZendeskConstants.Field.ID).asText());
                        }
                    }
                }
                else {
                    fetchData(jsonNode, target, task.getIncludes(), recordImporter, pageBuilder);
                }

                // only need to run one hit if in preview mode
                if (Exec.isPreview()) {
                    break;
                }
            }

            if (isNextIncrementalAvailable) {
                taskReport.set(ZendeskConstants.Field.START_TIME, endTimeStamp);
                taskReport.set(ZendeskConstants.Field.PREVIOUS_RECORDS, dedupRecordsBuilder.build());
            }
        }
        else {
            while (iterator.hasNext()) {
                fetchData(iterator.next(), target, task.getIncludes(), recordImporter, pageBuilder);

                if (Exec.isPreview()) {
                    break;
                }
            }
        }
        return taskReport;
    }

    @Override
    public ServiceDataSplitter<PluginTask> buildServiceDataSplitter(final PluginTask task)
    {
        return new ZendeskServiceDataSplitter<>(zendeskSupportAPiService);
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(final PluginTask task)
    {
        final JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        zendeskSupportAPiService = getZendeskV2SupportAPiService(getZendeskV2RestClient(task), task);

        if (task.getSchema() == null) {
            final Schema.Builder targetSchemaBuilder = new Schema.Builder();

            JsonNode jsonNode = zendeskSupportAPiService.getData("", 0, true);

            String targetJsonString = task.getTarget().toString();

            if (jsonNode.get(targetJsonString).isArray()) {
                final Iterator<JsonNode> iterator = jsonNode.get(targetJsonString).elements();
                // We always preview with only 1 record by option per_page=1
                if (iterator.hasNext()) {
                    final Iterator<Map.Entry<String, JsonNode>> iter = iterator.next().fields();

                    while (iter.hasNext()) {
                        final Map.Entry<String, JsonNode> entry = iter.next();
                        final Type columnType = ZendeskUtils.getColumnType(entry.getKey(), entry.getValue());

                        targetSchemaBuilder.add(entry.getKey(), columnType);

                        if (columnType == Types.TIMESTAMP) {
                            builder.add(new JacksonTimestampValueLocator(entry.getKey()), entry.getKey(), columnType,
                                    ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
                        }
                        else {
                            builder.add(entry.getKey(), columnType);
                        }
                    }

                    if (isSupportInclude(task.getTarget(), task.getIncludes())) {
                        for (String include : task.getIncludes()) {
                            targetSchemaBuilder.add(include, Types.JSON);
                            builder.add(include, Types.JSON);
                        }
                    }
                }
            }
            task.setSchema(targetSchemaBuilder.build());
        }
        else  {
            List<Column> columns = task.getSchema().getColumns();
            for (Column column : columns) {
                if (column.getType() != Types.TIMESTAMP) {
                    builder.add(column.getName(), column.getType());
                }
                else {
                    builder.add(new JacksonTimestampValueLocator(column.getName()), column.getName(), column.getType(),
                            ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
                }
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    protected static ZendeskRestClient getZendeskV2RestClient(final PluginTask task)
    {
        int retryLimit = Exec.isPreview() ? 1 : task.getRetryLimit();
        final Jetty92RetryHelper retryHelper = new Jetty92RetryHelper(retryLimit, task.getRetryInitialWaitSec() * 1000,
                task.getMaxRetryWaitSec() * 1000,
                new DefaultJetty92ClientCreator(task.getConnectionTimeout() * 1000, task.getConnectionTimeout() * 1000));
        return new ZendeskRestClientImpl(retryHelper, task);
    }

    @VisibleForTesting
    protected static ZendeskSupportAPiService getZendeskV2SupportAPiService(final ZendeskRestClient zendeskRestClient, final PluginTask task)
    {
        return new ZendeskSupportAPiServiceImpl(zendeskRestClient, task);
    }

    private boolean isLaterThanStartTime(String startTime, long time)
    {
        return time > ZendeskDateUtils.toTimeStamp(startTime);
    }

    private boolean isDuplicatedRecord(List<String> previousRecords, String recordId)
    {
        return previousRecords.contains(recordId);
    }

    private boolean isSupportInclude(Target target, List<String> includes)
    {
        return includes != null && !includes.isEmpty() && ZendeskUtils.isSupportInclude(target);
    }

    private void fetchData(final JsonNode jsonNode, final Target target, final List<String> includes,
                           final RecordImporter recordImporter, final PageBuilder pageBuilder)
    {
        if (isSupportInclude(target, includes)) {
            fetchRelatedObjects(jsonNode, target, includes, recordImporter, pageBuilder);
        }
        else {
            recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) getDataToImport(target, jsonNode)), pageBuilder);
        }
    }

    private void fetchRelatedObjects(final JsonNode jsonNode, final Target target, final List<String> includes,
                              final RecordImporter recordImporter, final PageBuilder pageBuilder)
    {
        // remove .json in url
        String url = jsonNode.get("url").asText().trim();

        if (url.length() > 5) {
            url = url.substring(0, url.length() - 5);
        }
        else {
            throw new DataException("Error when parsing url to fetch includes for " + jsonNode);
        }

        final StringBuilder builder = new StringBuilder(url);

        if (ZendeskUtils.isSupportInclude(target)) {
            builder.append("?include=");
            builder.append(includes
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.joining(",")));
        }

        final JsonNode result = zendeskSupportAPiService.getData(builder.toString(), 0, false);

        JsonNode targetJsonNode = result.get(target.getSingleFieldName());

        for (String include : includes) {
            JsonNode includeJsonNode = result.get(include);
            if (includeJsonNode != null) {
                ((ObjectNode) targetJsonNode).putPOJO(include, includeJsonNode);
            }
        }

        recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) targetJsonNode), pageBuilder);
    }

    private JsonNode getDataToImport(Target target, JsonNode jsonNode)
    {
        return target == Target.TICKET_METRICS
                ? jsonNode.get("metric_set")
                : jsonNode;
    }
}
