package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
        final Schema schema = task.getColumns().toSchema();
        int taskCount = 1;
        if (!ZendeskUtils.isSupportIncremental(task.getTarget())) {
            final JsonNode result = getZendeskSupportAPIService(task).getData("", 0, false, 0);
            if (result.get(ZendeskConstants.Field.COUNT) != null
                    && result.get(ZendeskConstants.Field.COUNT).isInt()) {
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
        ZendeskValidatorUtils.validateInputTask(task, getZendeskSupportAPIService(task));
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

    private ConfigDiff buildConfigDiff(final PluginTask task, final List<TaskReport> taskReports)
    {
        final ConfigDiff configDiff = Exec.newConfigDiff();

        if (!taskReports.isEmpty()) {
            final TaskReport taskReport = taskReports.get(0);
            if (ZendeskUtils.isSupportIncremental(task.getTarget())) {
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

    private AtomicInteger count = new AtomicInteger(0);
    private TaskReport ingestServiceData(final PluginTask task, final int taskIndex,
                                         final Schema schema, final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();

        long startTime = 0;

        if (ZendeskUtils.isSupportIncremental(task.getTarget())) {
            if (task.getStartTime().isPresent()) {
                startTime = ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get());
            }
        }
        Set<String> knownIDs = ConcurrentHashMap.newKeySet();
        while (true) {
            count.set(0);
            // Page start from 1 => page = taskIndex + 1
            final JsonNode result = getZendeskSupportAPIService(task).getData("", taskIndex + 1, false, startTime);
            String targetJsonName = task.getTarget().getJsonName();

            if (!result.has(targetJsonName) || !result.get(targetJsonName).isArray()) {
                throw new DataException(String.format("Missing '%s' from Zendesk API response", targetJsonName));
            }
            boolean isNextIncrementalAvailable = false;
            final Iterator<JsonNode> iterator = result.get(targetJsonName).elements();

            if (ZendeskUtils.isSupportIncremental(task.getTarget()) && !Exec.isPreview()) {
                final int numberOfRecords = result.get(ZendeskConstants.Field.COUNT).asInt();
                isNextIncrementalAvailable = numberOfRecords >= ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL;
                final long endTimeStamp = result.get(ZendeskConstants.Field.END_TIME).asLong();
                importDataForIncremental(iterator, task, knownIDs, schema, pageBuilder, startTime);
                taskReport.set(ZendeskConstants.Field.START_TIME, endTimeStamp);
            }
            else {
                while (iterator.hasNext()) {
                    fetchData(iterator.next(), task, schema, pageBuilder);
                    // in preview only need to hit one
                    if (Exec.isPreview()) {
                        break;
                    }
                }
            }

            if (ZendeskUtils.isSupportIncremental(task.getTarget())) {
                printCount();
            }

            // we need to break and do not store start_time when target is not incremental, next page is not available
            // or running in preview
            if (!isNextIncrementalAvailable || Exec.isPreview()) {
                break;
            }
            else {
                startTime = result.get(ZendeskConstants.Field.END_TIME).asLong();
            }
        }
        return taskReport;
    }

    private void printCount()
    {
        if (!Exec.isPreview()) {
            logger.info("Actual added '" + count + "' records");
        }
    }

    private void importDataForIncremental(final Iterator<JsonNode> iterator, final PluginTask task, Set<String> knownIDs,
                                          final Schema schema, final PageBuilder pageBuilder, final long startTime)
    {
        // create multiple threads for fetching the url with included objects of each records
        if (task.getIncludes().size() > 0 && ZendeskUtils.isSupportInclude(task.getTarget())) {
            ThreadPoolExecutor pool = null;
            try {
                pool = new ThreadPoolExecutor(
                        10, 100, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
                        );

                while (iterator.hasNext()) {
                    final JsonNode recordJsonNode = iterator.next();
                    pool.submit(() -> dedupAndFetchData(task, recordJsonNode, knownIDs, schema, pageBuilder, startTime));
                    // in preview only need to hit one
                    if (Exec.isPreview()) {
                        break;
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
                        throw new DataException(e);
                    }
                }
            }
        }
        else {
            while (iterator.hasNext()) {
                final JsonNode recordJsonNode = iterator.next();
                dedupAndFetchData(task, recordJsonNode, knownIDs, schema, pageBuilder, startTime);

                // in preview only need to hit one
                if (Exec.isPreview()) {
                    break;
                }
            }
        }
    }

    @VisibleForTesting
    protected void dedupAndFetchData(final PluginTask task, final JsonNode recordJsonNode, Set<String> knownIDs,
                                     final Schema schema, final PageBuilder pageBuilder, long startTime)
    {
        if (task.getDedup()) {
            String recordID = recordJsonNode.get(ZendeskConstants.Field.ID).asText();
            if (!knownIDs.contains(recordID) && !isUpdatedBySystem(recordJsonNode, startTime)) {
                fetchData(recordJsonNode, task, schema, pageBuilder);
                knownIDs.add(recordID);
                count.incrementAndGet();
            }
        }
        else {
            fetchData(recordJsonNode, task, schema, pageBuilder);
            count.incrementAndGet();
        }
    }

    private JsonNode buildColumns(final PluginTask task)
    {
        JsonNode jsonNode = getZendeskSupportAPIService(task).getData("", 0, true, 0);

        String targetName = task.getTarget().getJsonName();

        if (jsonNode.get(targetName) != null && jsonNode.get(targetName).isArray()) {
            return addAllColumnsToSchema(jsonNode, task.getTarget(), task.getIncludes());
        }
        else {
            throw new ConfigException("Could not guess schema due to empty data set");
        }
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

            if (name.equals("id")) {
                if (!type.equals(Types.LONG.getName())) {
                    entry.put("type", Types.LONG.getName());
                }
            }
            else if (idPattern.matcher(name).matches()) {
                entry.put("type", Types.STRING.getName());
            }

            if (type.equals(Types.TIMESTAMP.getName())) {
                entry.put("format", ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT);
            }
        }
        addIncludedObjectsToSchema((ArrayNode) columns, includes);
        return columns;
    }

    private void addIncludedObjectsToSchema(final ArrayNode objectNode, final List<String> includes)
    {
        final ObjectMapper mapper = new ObjectMapper();
        for (final String include : includes) {
            final ObjectNode object = mapper.createObjectNode();
            object.put("name", include);
            object.put("type", Types.JSON.getName());
            objectNode.add(object);
        }
    }

    private void fetchData(final JsonNode jsonNode, final PluginTask task, final Schema schema, final PageBuilder pageBuilder)
    {
        if (ZendeskUtils.isSupportInclude(task.getTarget(), task.getIncludes())) {
            fetchRelatedObjects(jsonNode, task, schema, pageBuilder);
        }
        else {
            ZendeskUtils.addRecord(jsonNode, schema, pageBuilder);
        }
    }

    private void fetchRelatedObjects(final JsonNode jsonNode, final PluginTask task, final Schema schema,
                                     final PageBuilder pageBuilder)
    {
        // remove .json in url
        final String url = jsonNode.get("url").asText().trim().replaceFirst("\\.json", "");

        final StringBuilder builder = new StringBuilder(url);

        final Target target = task.getTarget();
        if (ZendeskUtils.isSupportInclude(target)) {
            builder.append("?include=");
            builder.append(task.getIncludes()
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.joining(",")));
        }

        final JsonNode result = getZendeskSupportAPIService(task).getData(builder.toString(), 0, false, 0);
        final JsonNode targetJsonNode = result.get(target.getSingleFieldName());

        for (final String include : task.getIncludes()) {
            final JsonNode includeJsonNode = result.get(include);
            if (includeJsonNode != null) {
                ((ObjectNode) targetJsonNode).put(include, includeJsonNode);
            }
        }

        ZendeskUtils.addRecord(targetJsonNode, schema, pageBuilder);
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

    private ConfigSource createGuessConfig()
    {
        return Exec.newConfigSource()
                .set("guess_plugins", ImmutableList.of("zendesk"))
                .set("guess_sample_buffer_bytes", ZendeskConstants.Misc.GUESS_BUFFER_SIZE);
    }

    private JsonNode createSamples(final JsonNode jsonNode, final Target target)
    {
        JsonNode targetJsonNode = getJsonToGuess(jsonNode, target);

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

    private JsonNode getJsonToGuess(JsonNode jsonNode, final Target target)
    {
        jsonNode = Target.TICKET_METRICS.equals(target)
                ? jsonNode.get("metric_sets")
                : jsonNode.get(target.toString());

        return jsonNode;
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
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
        if (recordJsonNode.get(ZendeskConstants.Field.GENERATED_TIMESTAMP) != null
                && recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT) != null) {
            String recordUpdatedAtTime = recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText();
            long recordUpdatedAtToEpochSecond = ZendeskDateUtils.isoToEpochSecond(recordUpdatedAtTime);
            return recordUpdatedAtToEpochSecond <= startTime;
        }

        return false;
    }
}
