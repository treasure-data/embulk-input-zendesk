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
import org.embulk.input.zendesk.services.ZendeskCustomObjectService;
import org.embulk.input.zendesk.services.ZendeskNPSService;
import org.embulk.input.zendesk.services.ZendeskService;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.services.ZendeskUserEventService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Buffer;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

        @Config("object_types")
        @ConfigDefault("[]")
        List<String> getObjectTypes();

        @Config("relationship_types")
        @ConfigDefault("[]")
        List<String> getRelationshipTypes();

        @Config("profile_source")
        @ConfigDefault("null")
        Optional<String> getProfileSource();

        @Config("end_time")
        @ConfigDefault("null")
        Optional<String> getEndTime();

        @Config("user_event_type")
        @ConfigDefault("null")
        Optional<String> getUserEventType();

        @Config("user_event_source")
        @ConfigDefault("null")
        Optional<String> getUserEventSource();

        @Config("columns")
        SchemaConfig getColumns();
    }

    private ZendeskService zendeskService;

    private RecordImporter recordImporter;

    private static final Logger logger = Exec.getLogger(ZendeskInputPlugin.class);

    @Override
    public ConfigDiff transaction(final ConfigSource config, final Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
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
            final TaskReport taskReport = getZendeskService(task).addRecordToImporter(taskIndex, getRecordImporter(schema, pageBuilder));
            pageBuilder.finish();
            return taskReport;
        }
    }

    @Override
    public ConfigDiff guess(final ConfigSource config)
    {
        config.set("columns", new ObjectMapper().createArrayNode());
        final PluginTask task = config.loadConfig(PluginTask.class);
        validateInputTask(task);
        return Exec.newConfigDiff().set("columns", buildColumns(task));
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private ConfigDiff buildConfigDiff(final PluginTask task, final List<TaskReport> taskReports)
    {
        final ConfigDiff configDiff = Exec.newConfigDiff();

        if (!taskReports.isEmpty() && task.getIncremental()) {
            final TaskReport taskReport = taskReports.get(0);
            if (taskReport.has(ZendeskConstants.Field.START_TIME)) {
                final OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(
                        taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong()), ZoneOffset.UTC);

                configDiff.set(ZendeskConstants.Field.START_TIME,
                        offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT)));
            }

            if (taskReport.has(ZendeskConstants.Field.END_TIME)) {
                final OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(
                        taskReport.get(JsonNode.class, ZendeskConstants.Field.END_TIME).asLong()), ZoneOffset.UTC);

                configDiff.set(ZendeskConstants.Field.END_TIME,
                        offsetDateTime.format(DateTimeFormatter.ofPattern(ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT)));
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
        final JsonNode sample = new ObjectMapper().valueToTree(StreamSupport.stream(
                jsonNode.get(target.getJsonName()).spliterator(), false).limit(10).collect(Collectors.toList()));
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

    private void addIncludedObjectsToSchema(final ArrayNode arrayNode, final List<String> includes)
    {
        final ObjectMapper mapper = new ObjectMapper();

        includes.stream()
                .map((include) -> mapper.createObjectNode()
                        .put("name", include)
                        .put("type", Types.JSON.getName()))
                .forEach(arrayNode::add);
    }

    private ConfigSource createGuessConfig()
    {
        return Exec.newConfigSource()
                .set("guess_plugins", ImmutableList.of("zendesk"))
                .set("guess_sample_buffer_bytes", ZendeskConstants.Misc.GUESS_BUFFER_SIZE);
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
        if (task.getIncremental() && getZendeskService(task).isSupportIncremental())  {
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
            if (!task.getProfileSource().isPresent()) {
                throw new ConfigException("Profile Source is required for User Event Target");
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
}
