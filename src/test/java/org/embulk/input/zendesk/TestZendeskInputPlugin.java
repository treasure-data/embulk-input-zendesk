package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
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
import org.embulk.input.zendesk.utils.ZendeskPluginTestRuntime;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.embulk.input.zendesk.ZendeskInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.zendesk.ZendeskInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskInputPlugin
{
    @Rule
    public ZendeskPluginTestRuntime embulk = new ZendeskPluginTestRuntime();

    private ZendeskService zendeskSupportAPIService;

    private ZendeskInputPlugin zendeskInputPlugin;

    private ZendeskService zendeskChatService;

    private PageBuilder pageBuilder = mock(PageBuilder.class);

    private TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();

    @Before
    public void prepare()
    {
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        setupSupportAPIService();
        doReturn(pageBuilder).when(zendeskInputPlugin).getPageBuilder(any(Schema.class), any(PageOutput.class));
    }

    @Test
    public void testGuessGenerateColumnsForIncrementalTarget()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        setupTestGuessGenerateColumn(src, "data/tickets.json", "data/expected/ticket_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForIncrementalTargetIncludeRelatedObject()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        src.set("includes", Collections.singletonList("organizations"));
        setupTestGuessGenerateColumn(src, "data/tickets.json", "data/expected/ticket_column_with_related_objects.json");
    }

    @Test
    public void testGuessGenerateColumnsForTicketMetrics()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "ticket_metrics");
        setupTestGuessGenerateColumn(src, "data/ticket_metrics.json", "data/expected/ticket_metrics_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForUserEvents()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "user_events");
        setupTestGuessGenerateColumn(src, "data/user_event.json", "data/expected/user_events_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForChat()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("chat.yml");
        src.set("target", "chat");
        setupTestGuessGenerateColumn(src, "data/chat.json", "data/expected/chat_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForNonIncrementalTarget()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "ticket_fields");
        setupTestGuessGenerateColumn(src, "data/ticket_fields.json", "data/expected/ticket_fields_column.json");
    }

    @Test(expected = ConfigException.class)
    public void testGuessFail()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        loadData("data/error_data.json");

        zendeskInputPlugin.guess(src);
    }

    @Test
    public void testRunSupportAPINonIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("non-incremental.yml");
        loadData("data/ticket_fields.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        // running in 2 pages
        verify(pageBuilder, times(2)).finish();
        Assert.assertTrue(configDiff.isEmpty());
    }

    @Test
    public void testRunIncrementalStoreStartTimeAndEndTime()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml")
                .set("end_time", "2019-04-12 06:51:50 +0000");
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        taskReport.set(ZendeskConstants.Field.START_TIME, 1557026576);
        taskReport.set(ZendeskConstants.Field.END_TIME, 1560309776);

        when(zendeskSupportAPIService.isSupportIncremental()).thenReturn(true);
        when(zendeskSupportAPIService.addRecordToImporter(anyInt(), any())).thenReturn(taskReport);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        String nextEndTime = configDiff.get(String.class, ZendeskConstants.Field.END_TIME);
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-05-05 03:22:56 +0000", nextStartTime);
        assertEquals("2019-06-12 03:22:56 +0000", nextEndTime);
    }

    @Test
    public void testRunIncrementalStoreStartTimeAndEndTimeForChat()
    {
        setupChatService();
        final ConfigSource src = ZendeskTestHelper.getConfigSource("chat.yml")
            .set("incremental", true)
            .set("end_time", "2019-04-12 06:51:50 +0000");
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        taskReport.set(ZendeskConstants.Field.START_TIME, 1557026576);
        taskReport.set(ZendeskConstants.Field.END_TIME, 1560309776);

        when(zendeskChatService.isSupportIncremental()).thenReturn(true);
        when(zendeskChatService.addRecordToImporter(anyInt(), any())).thenReturn(taskReport);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        String nextEndTime = configDiff.get(String.class, ZendeskConstants.Field.END_TIME);
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-05-05 03:22:56 +0000", nextStartTime);
        assertEquals("2019-06-12 03:22:56 +0000", nextEndTime);
    }

    @Test
    public void testDispatchPerTargetShouldReturnSupportAPIService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        testReturnSupportAPIService(Target.TICKETS);
        testReturnSupportAPIService(Target.TICKET_METRICS);
        testReturnSupportAPIService(Target.TICKET_EVENTS);
        testReturnSupportAPIService(Target.TICKET_FORMS);
        testReturnSupportAPIService(Target.TICKET_FIELDS);
        testReturnSupportAPIService(Target.USERS);
        testReturnSupportAPIService(Target.ORGANIZATIONS);
    }

    @Test
    public void testDispatchPerTargetShouldReturn()
    {
        zendeskInputPlugin = spy(new ZendeskInputPlugin());

        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", Target.TICKETS.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        src.set("enable_cursor_based_api", true);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskCursorBasedService);

        src.set("target", Target.USERS.name().toLowerCase());
        task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskCursorBasedService);
    }

    @Test
    public void testDispatchPerTargetShouldReturnNPSService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        testReturnNPSService(Target.SCORES);
        testReturnNPSService(Target.RECIPIENTS);
    }

    @Test
    public void testDispatchPerTargetShouldReturnCustomObjectService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        testReturnCustomObjectService(Target.OBJECT_RECORDS);
        testReturnCustomObjectService(Target.RELATIONSHIP_RECORDS);
    }

    @Test
    public void testDispatchPerTargetShouldReturnChatService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        testReturnChatService(Target.CHAT);
    }

    @Test
    public void testDispatchPerTargetShouldReturnUserEventService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", Target.USER_EVENTS.name().toLowerCase());
        src.set("profile_source", "dummy");
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskUserEventService);
    }

    @Test
    public void validateCredentialOauthShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.set("auth_method", "oauth");
        configSource.remove("access_token");
        assertValidation(configSource, "access_token is required for authentication method 'oauth'");
    }

    @Test
    public void validateCredentialBasicShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.set("auth_method", "basic");
        configSource.remove("username");
        assertValidation(configSource, "username and password are required for authentication method 'basic'");

        configSource.set("username", "");
        configSource.remove("password");
        assertValidation(configSource, "username and password are required for authentication method 'basic'");
    }

    @Test
    public void validateCredentialTokenShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.set("auth_method", "token");
        configSource.remove("token");
        assertValidation(configSource, "username and token are required for authentication method 'token'");

        configSource.set("token", "");
        configSource.remove("username");
        assertValidation(configSource, "username and token are required for authentication method 'token'");
    }

    @Test
    public void validateAppMarketPlaceShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.remove("app_marketplace_integration_name");
        assertValidation(configSource, "All of app_marketplace_integration_name, app_marketplace_org_id, " +
                "app_marketplace_app_id " +
                "are required to fill out for Apps Marketplace API header");
    }

    @Test
    public void validateCustomObjectShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.set("target", Target.OBJECT_RECORDS.name().toLowerCase());
        assertValidation(configSource, "Should have at least one Object Type");

        configSource.set("target", Target.RELATIONSHIP_RECORDS.name().toLowerCase());
        assertValidation(configSource, "Should have at least one Relationship Type");
    }

    @Test
    public void validateUserEventShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        configSource.set("target", Target.USER_EVENTS.name().toLowerCase());
        configSource.set("user_event_type", "user_event_type");
        configSource.remove("user_event_source");
        assertValidation(configSource, "User Profile Source is required when filtering by User Event Type");
    }

    @Test
    public void validateTimeShouldThrowException()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        when(zendeskSupportAPIService.isSupportIncremental()).thenReturn(true);
        configSource.set("target", Target.TICKETS.name().toLowerCase());
        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", "2019-12-2 22-12-22");
        assertValidation(configSource, "End Time should follow these format " + ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT.toString());

        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() + 3600), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        assertValidation(configSource, "End Time should be later or equal than Start Time");
    }

    @Test
    public void isValidTimeRangeShouldThrowException()
    {
        ZendeskTestHelper.setPreviewMode(embulk, true);
        String expectedMessage = "Invalid End time. End time is greater than current time";
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        when(zendeskSupportAPIService.isSupportIncremental()).thenReturn(true);
        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() + 1000), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));

        assertValidation(configSource, expectedMessage);

        try {
            zendeskInputPlugin.transaction(configSource, new Control());
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    @Test
    public void runShouldKeepOldStartTimeAndEndTimeInConfigDiff()
    {
        ConfigSource configSource = ZendeskTestHelper.getConfigSource("base_validator.yml");
        ZendeskTestHelper.setPreviewMode(embulk, false);
        when(zendeskSupportAPIService.isSupportIncremental()).thenReturn(true);
        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() + 1000), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));

        ConfigDiff configDiff = zendeskInputPlugin.transaction(configSource, new Control());
        assertEquals(ZendeskDateUtils.convertToDateTimeFormat(configSource.get(String.class, ZendeskConstants.Field.START_TIME), ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT),
                configDiff.get(String.class, ZendeskConstants.Field.START_TIME));
        assertEquals(ZendeskDateUtils.convertToDateTimeFormat(configSource.get(String.class, ZendeskConstants.Field.END_TIME), ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT),
                configDiff.get(String.class, ZendeskConstants.Field.END_TIME));
    }

    private void assertValidation(final ConfigSource configSource, final String message)
    {
        try {
            zendeskInputPlugin.guess(configSource);
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(message, e.getMessage());
        }
    }

    private void testReturnSupportAPIService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskSupportAPIService);
    }

    private void testReturnNPSService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskNPSService);
    }

    private void testReturnCustomObjectService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("relationship_types", Collections.singletonList("dummy"));
        src.set("object_types", Collections.singletonList("account"));
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskCustomObjectService);
    }

    private void testReturnChatService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = CONFIG_MAPPER.map(src, ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskChatService);
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskSupportAPIService.getDataFromPath(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJson);
    }

    private void setupTestGuessGenerateColumn(ConfigSource src, String fileName, String expectedSource)
    {
        loadData(fileName);
        ConfigDiff configDiff = zendeskInputPlugin.guess(src);
        List<String> actualValues = new ArrayList<>();
        JsonNode columns = configDiff.get(JsonNode.class, "columns");
        Iterator<JsonNode> it = columns.elements();
        while (it.hasNext()) {
            JsonNode actual = it.next();
            actualValues.add(actual.toString());
        }
        JsonNode expectedNodes = ZendeskTestHelper.getJsonFromFile(expectedSource);
        it = expectedNodes.elements();
        while (it.hasNext()) {
            JsonNode expectedNode = it.next();
            assertTrue("Missing expected node: " + expectedNode.toString(), actualValues.contains(expectedNode.toString()));
        }
    }

    private void setupSupportAPIService()
    {
        zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);
        doReturn(zendeskSupportAPIService).when(zendeskInputPlugin).dispatchPerTarget(any(ZendeskInputPlugin.PluginTask.class));
    }

    private void setupChatService()
    {
        zendeskChatService = mock(ZendeskChatService.class);
        doReturn(zendeskChatService).when(zendeskInputPlugin).dispatchPerTarget(any(ZendeskInputPlugin.PluginTask.class));
    }

    private class Control implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(final TaskSource taskSource, final Schema schema, final int taskCount)
        {
            List<TaskReport> reports = IntStream.range(0, taskCount)
                    .mapToObj(i -> zendeskInputPlugin.run(taskSource, schema, i, output))
                    .collect(Collectors.toList());
            return reports;
        }
    }
}
