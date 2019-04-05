package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.GuessExecutor;
import org.embulk.input.zendesk.models.AuthenticationMethod;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.input.zendesk.utils.ZendeskValidatorUtils;
import org.embulk.spi.Buffer;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ZendeskInputPlugin implements InputPlugin
{
    public interface PluginTask extends Task
    {
        @Config("login_url")
        String getLoginUrl();

        @Config("auth_method")
        @ConfigDefault("\"basic\"")
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
        @ConfigDefault("4")
        int getRetryInitialWaitSec();

        @Min(30)
        @Max(3600)
        @Config("max_retry_wait_sec")
        @ConfigDefault("60")
        int getMaxRetryWaitSec();

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

        @Config("columns")
        SchemaConfig getColumns();
    }

    private static final Logger logger = Exec.getLogger(ZendeskInputPlugin.class);

    private ZendeskSupportAPIService zendeskSupportAPIService;

    @Override
    public ConfigDiff transaction(final ConfigSource config, final Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task, getZendeskSupportAPIService(task));
        final Schema schema = task.getColumns().toSchema();
        int taskCount = 1;

        // For non-incremental target, we will split records based on number of pages. 100 records per page
        // In preview, run with taskCount = 1
        if (!ZendeskUtils.isSupportAPIIncremental(task.getTarget()) && !Exec.isPreview()) {
            final JsonNode result = getZendeskSupportAPIService(task).getData("", 0, false, 0);
            if (result.has(ZendeskConstants.Field.COUNT) && result.get(ZendeskConstants.Field.COUNT).isInt()) {
                taskCount = ZendeskUtils.numberToSplitWithHintingInTask(result.get(ZendeskConstants.Field.COUNT).asInt());
            }
        }
        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(final TaskSource taskSource, final Schema schema, final int taskCount, final Control control)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final List<TaskReport> taskReports = control.run(taskSource, schema, taskCount);
        return this.buildConfigDiff(task, taskReports);
    }

    @Override
    public void cleanup(final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(final TaskSource taskSource, final Schema schema, final int taskIndex, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
            final TaskReport taskReport = ingestServiceData(task, taskIndex, schema, pageBuilder);
            pageBuilder.finish();
            return taskReport;
        }
    }

    @Override
    public ConfigDiff guess(final ConfigSource config)
    {
        config.set("columns", new ObjectMapper().createArrayNode());
        final PluginTask task = config.loadConfig(PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task, getZendeskSupportAPIService(task));
        return Exec.newConfigDiff().set("columns", buildColumns(task));
    }

    @VisibleForTesting
    protected ZendeskSupportAPIService getZendeskSupportAPIService(final PluginTask task)
    {
        if (this.zendeskSupportAPIService == null) {
            this.zendeskSupportAPIService = new ZendeskSupportAPIService(task);
        }
        this.zendeskSupportAPIService.setTask(task);
        return this.zendeskSupportAPIService;
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private ConfigDiff buildConfigDiff(final PluginTask task, final List<TaskReport> taskReports)
    {
        final ConfigDiff configDiff = Exec.newConfigDiff();

        if (!taskReports.isEmpty()) {
            if (ZendeskUtils.isSupportAPIIncremental(task.getTarget())) {
                final TaskReport taskReport = taskReports.get(0);
                if (taskReport.has(ZendeskConstants.Field.START_TIME)) {
                    final OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(
                            taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong()),
                            ZoneOffset.UTC);

                    configDiff.set(ZendeskConstants.Field.START_TIME,
                            offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT)));
                }
            }
        }
        return configDiff;
    }

    private TaskReport ingestServiceData(final PluginTask task, final int taskIndex,
                                         final Schema schema, final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();

        if (ZendeskUtils.isSupportAPIIncremental(task.getTarget())) {
            importDataForIncremental(task, schema, pageBuilder, taskReport);
        }
        else {
            importDataForNonIncremental(task, taskIndex, schema, pageBuilder);
        }

        return taskReport;
    }

    private void importDataForIncremental(final PluginTask task, final Schema schema,
                                          final PageBuilder pageBuilder, final TaskReport taskReport)
    {
        long startTime = 0;

        if (ZendeskUtils.isSupportAPIIncremental(task.getTarget()) && task.getStartTime().isPresent()) {
                startTime = ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get());
        }

        // For incremental target, we will run in one task but split in multiple threads inside for data deduplication.
        // Run with incremental will contain duplicated data.
        ThreadPoolExecutor pool = null;
        try {
            Set<String> knownIds = ConcurrentHashMap.newKeySet();
            pool = new ThreadPoolExecutor(
                    10, 100, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
            );

            while (true) {
                int recordCount = 0;
                final JsonNode result = getZendeskSupportAPIService(task).getData("", 0, false, startTime);
                final Iterator<JsonNode> iterator = getListRecords(result, task.getTarget().getJsonName());

                int numberOfRecords = 0;
                if (result.has(ZendeskConstants.Field.COUNT)) {
                    numberOfRecords = result.get(ZendeskConstants.Field.COUNT).asInt();
                }

                while (iterator.hasNext()) {
                    final JsonNode recordJsonNode = iterator.next();

                    if (isUpdatedBySystem(recordJsonNode, startTime)) {
                       continue;
                    }

                    if (task.getDedup()) {
                        String recordID = recordJsonNode.get(ZendeskConstants.Field.ID).asText();
                        if (knownIds.contains(recordID)) {
                            continue;
                        }
                        else {
                            knownIds.add(recordID);
                        }
                    }

                    pool.submit(() -> fetchData(recordJsonNode, task, schema, pageBuilder));
                    recordCount++;
                    if (Exec.isPreview()) {
                        return;
                    }
                }
                logger.info("Fetched '{}' records from start_time '{}'", recordCount, startTime);
                if (result.has(ZendeskConstants.Field.END_TIME) && !result.get(ZendeskConstants.Field.END_TIME).isNull()
                        && result.has(task.getTarget().getJsonName())) {
                    // NOTE: start_time compared as "=>", not ">".
                    // If we will use end_time for next start_time, we got the same record that is last fetched
                    // end_time + 1 is workaround for that
                    taskReport.set("start_time", result.get(ZendeskConstants.Field.END_TIME).asLong() + 1);
                }
                else {
                    // Sometimes no record and no end_time fetched on the job, but we should generate start_time on config_diff.
                    taskReport.set("start_time", Instant.now().getEpochSecond());
                }

                if (numberOfRecords < ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL) {
                    break;
                }
                else {
                    startTime = result.get(ZendeskConstants.Field.END_TIME).asLong();
                }
            }
        }
        finally {
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
                catch (final InterruptedException e) {
                    logger.warn("Error when wait pool to finish");
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private void importDataForNonIncremental(final PluginTask task, final int taskIndex, final Schema schema,
                                             final PageBuilder pageBuilder)
    {
        // Page start from 1 => page = taskIndex + 1
        final JsonNode result = getZendeskSupportAPIService(task).getData("", taskIndex + 1, false, 0);
        final Iterator<JsonNode> iterator = getListRecords(result, task.getTarget().getJsonName());

        while (iterator.hasNext()) {
            fetchData(iterator.next(), task, schema, pageBuilder);

            if (Exec.isPreview()) {
                break;
            }
        }
    }

    private Iterator<JsonNode> getListRecords(JsonNode result, String targetJsonName)
    {
        if (!result.has(targetJsonName) || !result.get(targetJsonName).isArray()) {
            throw new DataException(String.format("Missing '%s' from Zendesk API response", targetJsonName));
        }
        return result.get(targetJsonName).elements();
    }

    private JsonNode buildColumns(final PluginTask task)
    {
        JsonNode jsonNode = getZendeskSupportAPIService(task).getData("", 0, true, 0);

        String targetName = task.getTarget().getJsonName();

        if (jsonNode.has(targetName) && jsonNode.get(targetName).isArray()) {
            return addAllColumnsToSchema(jsonNode, task.getTarget(), task.getIncludes());
        }
        throw new ConfigException("Could not guess schema due to empty data set");
    }

    private final Pattern idPattern = Pattern.compile(ZendeskConstants.Regex.ID);
    private JsonNode addAllColumnsToSchema(final JsonNode jsonNode, final Target target, final List<String> includes)
    {
        final JsonNode sample = createSamples(jsonNode, target);
        final Buffer bufferSample = Buffer.copyOf(sample.toString().getBytes(StandardCharsets.UTF_8));
        final JsonNode columns = Exec.getInjector().getInstance(GuessExecutor.class)
                .guessParserConfig(bufferSample, Exec.newConfigSource(), createGuessConfig())
                .getObjectNode().get("columns");

        final Iterator<JsonNode> ite = columns.elements();

        while (ite.hasNext()) {
            final ObjectNode entry = (ObjectNode) ite.next();
            final String name = entry.get("name").asText();
            final String type = entry.get("type").asText();

            if (type.equals(Types.TIMESTAMP.getName())) {
                entry.put("format", ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
            }

            if (name.equals("id")) {
                if (!type.equals(Types.LONG.getName())) {
                    if (type.equals(Types.TIMESTAMP.getName())) {
                        entry.remove("format");
                    }
                    entry.put("type", Types.LONG.getName());
                }
            }
            else if (idPattern.matcher(name).matches()) {
                if (type.equals(Types.TIMESTAMP.getName())) {
                    entry.remove("format");
                }
                entry.put("type", Types.STRING.getName());
            }
        }
        addIncludedObjectsToSchema((ArrayNode) columns, includes);
        return columns;
    }

    private void addIncludedObjectsToSchema(final ArrayNode arrayNode, final List<String> includes)
    {
        final ObjectMapper mapper = new ObjectMapper();

        includes.stream()
                .map((include) -> mapper.createObjectNode()
                        .put("name", include)
                        .put("type", Types.JSON.getName()))
                .forEach(arrayNode::add);
    }

    private void fetchData(final JsonNode jsonNode, final PluginTask task, final Schema schema,
                                     final PageBuilder pageBuilder)
    {
        // FIXME:  if include is not contained in schema, data should be ignore
        task.getIncludes().forEach(include -> {
            String relatedObjectName = include.trim();
            final String url = task.getLoginUrl() + ZendeskConstants.Url.API
                    + "/" + task.getTarget().toString()
                    + "/" + jsonNode.get(ZendeskConstants.Field.ID).asText()
                    + "/" + relatedObjectName + ".json";

            try {
                final JsonNode result = getZendeskSupportAPIService(task).getData(url, 0, false, 0);
                if (result != null && result.has(relatedObjectName)) {
                    ((ObjectNode) jsonNode).set(include, result.get(relatedObjectName));
                }
            }
            catch (final ConfigException e) {
                // Sometimes we get 404 when having invalid endpoint, so ignore when we get 404 InvalidEndpoint
                if (!e.getMessage().contains(ZendeskConstants.Misc.INVALID_END_POINT_RESPONSE)) {
                    throw e;
                }
            }
        });
        ZendeskUtils.addRecord(jsonNode, schema, pageBuilder);
    }

    private ConfigSource createGuessConfig()
    {
        return Exec.newConfigSource()
                .set("guess_plugins", ImmutableList.of("zendesk"))
                .set("guess_sample_buffer_bytes", ZendeskConstants.Misc.GUESS_BUFFER_SIZE);
    }

    private JsonNode createSamples(final JsonNode jsonNode, final Target target)
    {
        JsonNode targetJsonNode = jsonNode.get(target.getJsonName());

        if (targetJsonNode.isArray() && targetJsonNode.size() > 0) {
            // prevent over buffer size
            ArrayNode arrayNode = new ObjectMapper().createArrayNode();
            for (int i = 0; i < targetJsonNode.size(); i++) {
                arrayNode.add(targetJsonNode.get(i));
                if (i >= 10) {
                    break;
                }
            }
            return arrayNode;
        }
        throw new ConfigException("Could not guess schema due to empty data set");
    }

    private boolean isUpdatedBySystem(JsonNode recordJsonNode, long startTime)
    {
        /*
         * https://developer.zendesk.com/rest_api/docs/core/incremental_export#excluding-system-updates
         * "generated_timestamp" will be updated when Zendesk internal changing
         * "updated_at" will be updated when ticket data was changed
         * start_time for query parameter will be processed on Zendesk with generated_timestamp,
         * but it was calculated by record' updated_at time.
         * So the doesn't changed record from previous import would be appear by Zendesk internal changes.
         * We ignore record that has updated_at <= start_time
         */
        if (recordJsonNode.has(ZendeskConstants.Field.GENERATED_TIMESTAMP) && recordJsonNode.has(ZendeskConstants.Field.UPDATED_AT)) {
            String recordUpdatedAtTime = recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText();
            long recordUpdatedAtToEpochSecond = ZendeskDateUtils.isoToEpochSecond(recordUpdatedAtTime);
            return recordUpdatedAtToEpochSecond <= startTime;
        }

        return false;
    }
}
