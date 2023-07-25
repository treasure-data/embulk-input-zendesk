package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.zendesk.models.AuthenticationMethod;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskChatService;
import org.embulk.input.zendesk.services.ZendeskCursorBasedService;
import org.embulk.input.zendesk.services.ZendeskCustomObjectService;
import org.embulk.input.zendesk.services.ZendeskNPSService;
import org.embulk.input.zendesk.services.ZendeskService;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.services.ZendeskUserEventService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ColumnModule;
import org.embulk.util.config.modules.SchemaModule;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.guess.SchemaGuess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class ZendeskInputPlugin
    implements InputPlugin
{
    public interface PluginTask
        extends Task
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

        @Config("object_types")
        @ConfigDefault("[]")
        List<String> getObjectTypes();

        @Config("relationship_types")
        @ConfigDefault("[]")
        List<String> getRelationshipTypes();

        @Config("end_time")
        @ConfigDefault("null")
        Optional<String> getEndTime();

        @Config("user_event_type")
        @ConfigDefault("null")
        Optional<String> getUserEventType();

        @Config("user_event_source")
        @ConfigDefault("null")
        Optional<String> getUserEventSource();

        @Config("enable_cursor_based_api")
        @ConfigDefault("false")
        boolean getEnableCursorBasedApi();

        @Config("columns")
        SchemaConfig getColumns();
    }

    private ZendeskService zendeskService;

    private RecordImporter recordImporter;

    private static final Logger logger = LoggerFactory.getLogger(ZendeskInputPlugin.class);

    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder()
        .addDefaultModules()
        .addModule(new SchemaModule())
        .addModule(new ColumnModule())
        .addModule(new TypeModule())
        .build();

    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    @Override
    public ConfigDiff transaction(final ConfigSource config, final Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        validateInputTask(task);

        final Schema schema = task.getColumns().toSchema();
        int taskCount = 1;

        // For non-incremental target, we will split records based on number of pages. 100 records per page
        // In preview, run with taskCount = 1
        if (!Exec.isPreview() && !getZendeskService(task).isSupportIncremental() && getZendeskService(task) instanceof ZendeskSupportAPIService) {
            final JsonNode result = getZendeskService(task).getDataFromPath("", 0, false, 0);
            if (result.has(ZendeskConstants.Field.COUNT) && !result.get(ZendeskConstants.Field.COUNT).isNull()
                && result.get(ZendeskConstants.Field.COUNT).isInt()) {
                taskCount = ZendeskUtils.numberToSplitWithHintingInTask(result.get(ZendeskConstants.Field.COUNT).asInt());
            }
        }
        return resume(task.toTaskSource(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(final TaskSource taskSource, final Schema schema, final int taskCount, final Control control)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
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
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        if (getZendeskService(task).isSupportIncremental() && !isValidTimeRange(task)) {
            if (Exec.isPreview()) {
                throw new ConfigException("Invalid End time. End time is greater than current time");
            }

            logger.warn("The end time, '" + task.getEndTime().get() + "', is greater than the current time. No records will be imported");

            // we just need to store config_diff when incremental_mode is enable
            if (task.getIncremental()) {
                return buildTaskReportKeepOldStartAndEndTime(task);
            }
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
            final TaskReport taskReport = getZendeskService(task).addRecordToImporter(taskIndex, getRecordImporter(schema, pageBuilder));
            pageBuilder.finish();
            return taskReport;
        }
    }

    @Override
    public ConfigDiff guess(final ConfigSource config)
    {
        config.set("columns", new ArrayList<>());
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        validateInputTask(task);
        if (!isValidTimeRange(task)) {
            throw new ConfigException("Invalid End time. End time is greater than current time");
        }
        return CONFIG_MAPPER_FACTORY.newConfigDiff().set("columns", buildColumns(task));
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return Exec.getPageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private ConfigDiff buildConfigDiff(final PluginTask task, final List<TaskReport> taskReports)
    {
        final ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        if (!taskReports.isEmpty() && task.getIncremental()) {
            final TaskReport taskReport = taskReports.get(0);
            if (taskReport.has(ZendeskConstants.Field.START_TIME)) {
                final Long startTime = taskReport.get(Long.class, ZendeskConstants.Field.START_TIME);
                if (startTime != null) {
                    final OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(startTime), ZoneOffset.UTC);
                    configDiff.set(ZendeskConstants.Field.START_TIME,
                        offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT)));
                }
            }

            if (taskReport.has(ZendeskConstants.Field.END_TIME)) {
                final Long endTime = taskReport.get(Long.class, ZendeskConstants.Field.END_TIME);
                if (endTime != null) {
                    final OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(endTime), ZoneOffset.UTC);
                    configDiff.set(ZendeskConstants.Field.END_TIME,
                        offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT)));
                }
            }
        }
        return configDiff;
    }

    private JsonNode buildColumns(final PluginTask task)
    {
        JsonNode jsonNode = getZendeskService(task).getDataFromPath("", 0, true, 0);

        String targetName = task.getTarget().getJsonName();

        if (jsonNode.has(targetName) && !jsonNode.get(targetName).isNull() && jsonNode.get(targetName).isArray() && jsonNode.get(targetName).size() > 0) {
            return addAllColumnsToSchema(jsonNode, task.getTarget(), task.getIncludes());
        }
        throw new ConfigException("Could not guess schema due to empty data set");
    }

    private final Pattern idPattern = Pattern.compile(ZendeskConstants.Regex.ID);

    private JsonNode addAllColumnsToSchema(final JsonNode jsonNode, final Target target, final List<String> includes)
    {
        ConfigDiff configDiff = guessData(jsonNode, target.getJsonName());
        ConfigDiff parser = configDiff.getNested("parser");
        if (parser.has("columns")) {
            JsonNode columns = parser.get(JsonNode.class, "columns");
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

                    // Id of User Events target is more suitable for String
                    if (target.equals(Target.USER_EVENTS)) {
                        entry.put("type", Types.STRING.getName());
                    }
                }
                else if (idPattern.matcher(name).find()) {
                    if (type.equals(Types.TIMESTAMP.getName())) {
                        entry.remove("format");
                    }
                    entry.put("type", Types.STRING.getName());
                }
            }
            addIncludedObjectsToSchema((ArrayNode) columns, includes);
            return columns;
        }
        else {
            throw new ConfigException("Fail to guess column");
        }
    }

    @VisibleForTesting
    protected ConfigDiff guessData(final JsonNode jsonNode, final String targetJsonName)
    {
        List<String> unifiedFieldNames = unifiedFieldNames(jsonNode, targetJsonName);
        List<List<Object>> samples = createSampleData(jsonNode, targetJsonName, unifiedFieldNames);
        List<ConfigDiff> columnConfigs = SchemaGuess.of(CONFIG_MAPPER_FACTORY).fromListRecords(unifiedFieldNames, samples);
        if (columnConfigs.isEmpty()) {
            throw new ConfigException("Fail to guess column");
        }
        columnConfigs.forEach(conf -> conf.remove("index"));
        ConfigDiff parserGuessed = CONFIG_MAPPER_FACTORY.newConfigDiff();
        parserGuessed.set("columns", columnConfigs);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();
        configDiff.setNested("parser", parserGuessed);
        return configDiff;
    }

    private List<List<Object>> createSampleData(JsonNode jsonNode, String targetJsonName, List<String> unifiedFieldNames)
    {
        final List<List<Object>> samples = new ArrayList<>();
        Iterator<JsonNode> records = ZendeskUtils.getListRecords(jsonNode, targetJsonName);
        while (records.hasNext()) {
            JsonNode node = records.next();
            List<Object> line = new ArrayList<>();
            for (String field : unifiedFieldNames) {
                JsonNode childNode = node.get(field);
                if (childNode == null || childNode.isNull() || "null".equals(childNode.asText())) {
                    line.add(null);
                    continue;
                }
                if (childNode.isContainerNode()) {
                    line.add(childNode);
                }
                else {
                    line.add(childNode.asText());
                }
            }
            samples.add(line);
        }
        return samples;
    }

    private List<String> unifiedFieldNames(JsonNode jsonNode, String targetJsonName)
    {
        List<String> columnNames = new ArrayList<>();
        Iterator<JsonNode> records = ZendeskUtils.getListRecords(jsonNode, targetJsonName);
        while (records.hasNext()) {
            Iterator<String> fieldNames = records.next().fieldNames();
            while (fieldNames.hasNext()) {
                String field = fieldNames.next();
                if (!columnNames.contains(field)) {
                    columnNames.add(field);
                }
            }
        }
        return columnNames;
    }

    /**
     * Method to read sample bytes of the csv file
     */
    private byte[] readSampleBytes(String downloadedFilePath)
    {
        final int sampleBytes = 64000;
        final ByteArrayOutputStream bos = new ByteArrayOutputStream(sampleBytes);

        byte[] buffer = new byte[sampleBytes];
        try (FileInputStream inputStream = new FileInputStream(downloadedFilePath)) {
            int read = inputStream.read(buffer, 0, sampleBytes);
            bos.write(buffer, 0, read);
        }
        catch (IOException e) {
            throw new DataException(e);
        }
        return bos.toByteArray();
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

    private ZendeskService getZendeskService(PluginTask task)
    {
        if (zendeskService == null) {
            zendeskService = dispatchPerTarget(task);
        }
        return zendeskService;
    }

    @VisibleForTesting
    protected ZendeskService dispatchPerTarget(ZendeskInputPlugin.PluginTask task)
    {
        switch (task.getTarget()) {
            case TICKETS:
            case USERS:
                /*
                 The cursor based incremental API is enabled only tickets and users targets
                 It allows to fetch more than 10.000 records which is now the limitation of the old incremental api
                 https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/#cursor-based-incremental-exports
                */
                return task.getEnableCursorBasedApi() ? new ZendeskCursorBasedService(task) : new ZendeskSupportAPIService(task);
            case ORGANIZATIONS:
            case TICKET_METRICS:
            case TICKET_EVENTS:
            case TICKET_FORMS:
            case TICKET_FIELDS:
                return new ZendeskSupportAPIService(task);
            case RECIPIENTS:
            case SCORES:
                return new ZendeskNPSService(task);
            case OBJECT_RECORDS:
            case RELATIONSHIP_RECORDS:
                return new ZendeskCustomObjectService(task);
            case USER_EVENTS:
                return new ZendeskUserEventService(task);
            case CHAT:
                return new ZendeskChatService(task);
            default:
                throw new ConfigException("Unsupported " + task.getTarget() + ", supported values: '" + Arrays.toString(Target.values()) + "'");
        }
    }

    private RecordImporter getRecordImporter(Schema schema, PageBuilder pageBuilder)
    {
        if (recordImporter == null) {
            recordImporter = new RecordImporter(schema, pageBuilder);
        }
        return recordImporter;
    }

    private void validateInputTask(PluginTask task)
    {
        validateAppMarketPlace(task.getAppMarketPlaceIntegrationName().isPresent(),
            task.getAppMarketPlaceAppId().isPresent(),
            task.getAppMarketPlaceOrgId().isPresent());
        validateCredentials(task);
        validateIncremental(task);
        validateCustomObject(task);
        validateUserEvent(task);
        validateTime(task);
    }

    private void validateCredentials(PluginTask task)
    {
        switch (task.getAuthenticationMethod()) {
            case OAUTH:
                if (!task.getAccessToken().isPresent()) {
                    throw new ConfigException(String.format("access_token is required for authentication method '%s'",
                        task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            case TOKEN:
                if (!task.getUsername().isPresent() || !task.getToken().isPresent()) {
                    throw new ConfigException(String.format("username and token are required for authentication method '%s'",
                        task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            case BASIC:
                if (!task.getUsername().isPresent() || !task.getPassword().isPresent()) {
                    throw new ConfigException(String.format("username and password are required for authentication method '%s'",
                        task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            default:
                throw new ConfigException("Unknown authentication method");
        }
    }

    private void validateAppMarketPlace(final boolean isAppMarketIntegrationNamePresent,
        final boolean isAppMarketAppIdPresent,
        final boolean isAppMarketOrgIdPresent)
    {
        final boolean isAllAvailable =
            isAppMarketIntegrationNamePresent && isAppMarketAppIdPresent && isAppMarketOrgIdPresent;
        final boolean isAllUnAvailable =
            !isAppMarketIntegrationNamePresent && !isAppMarketAppIdPresent && !isAppMarketOrgIdPresent;
        // All or nothing needed
        if (!(isAllAvailable || isAllUnAvailable)) {
            throw new ConfigException("All of app_marketplace_integration_name, app_marketplace_org_id, " +
                "app_marketplace_app_id " +
                "are required to fill out for Apps Marketplace API header");
        }
    }

    private void validateIncremental(PluginTask task)
    {
        if (task.getIncremental() && getZendeskService(task).isSupportIncremental()) {
            if (!task.getDedup()) {
                logger.warn("You've selected to skip de-duplicating records, result may contain duplicated data");
            }

            if (!getZendeskService(task).isSupportIncremental() && task.getStartTime().isPresent()) {
                logger.warn(String.format("Target: '%s' doesn't support incremental export API. Will be ignored start_time option",
                    task.getTarget()));
            }
        }
    }

    private void validateCustomObject(PluginTask task)
    {
        if (task.getTarget().equals(Target.OBJECT_RECORDS) && task.getObjectTypes().isEmpty()) {
            throw new ConfigException("Should have at least one Object Type");
        }

        if (task.getTarget().equals(Target.RELATIONSHIP_RECORDS) && task.getRelationshipTypes().isEmpty()) {
            throw new ConfigException("Should have at least one Relationship Type");
        }
    }

    private void validateUserEvent(PluginTask task)
    {
        if (task.getTarget().equals(Target.USER_EVENTS)) {
            if (task.getUserEventType().isPresent() && !task.getUserEventSource().isPresent()) {
                throw new ConfigException("User Profile Source is required when filtering by User Event Type");
            }
        }
    }

    private void validateTime(PluginTask task)
    {
        if (getZendeskService(task).isSupportIncremental()) {
            // Can't set end_time to 0, so it should be valid
            task.getEndTime().ifPresent(time -> {
                if (!ZendeskDateUtils.supportedTimeFormat(task.getEndTime().get()).isPresent()) {
                    throw new ConfigException("End Time should follow these format " + ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT.toString());
                }
            });

            if (task.getStartTime().isPresent() && task.getEndTime().isPresent()
                && ZendeskDateUtils.getStartTime(task.getStartTime().get()) > ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get())) {
                throw new ConfigException("End Time should be later or equal than Start Time");
            }
        }
    }

    private boolean isValidTimeRange(PluginTask task)
    {
        return !task.getEndTime().isPresent() || ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get()) <= Instant.now().getEpochSecond();
    }

    private TaskReport buildTaskReportKeepOldStartAndEndTime(PluginTask task)
    {
        final TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();

        if (task.getStartTime().isPresent()) {
            taskReport.set(ZendeskConstants.Field.START_TIME, ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get()));
        }

        if (task.getEndTime().isPresent()) {
            taskReport.set(ZendeskConstants.Field.END_TIME, ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get()));
        }

        return taskReport;
    }
}
