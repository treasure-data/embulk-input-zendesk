package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskCustomObjectService;
import org.embulk.input.zendesk.services.ZendeskNPSService;
import org.embulk.input.zendesk.services.ZendeskService;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.services.ZendeskUserEventService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskPluginTestRuntime;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;

import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.spi.TestPageBuilderReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;
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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestZendeskInputPlugin
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public ZendeskPluginTestRuntime embulk = new ZendeskPluginTestRuntime();

    private ZendeskService zendeskSupportAPIService;

    private ZendeskInputPlugin zendeskInputPlugin;

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
        src.set("profile_source", "dummy");
        setupTestGuessGenerateColumn(src, "data/user_event.json", "data/expected/user_events_column.json");
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
    public void testRunIncrementalStoreStartTime()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        TaskReport taskReport = Exec.newTaskReport();
        taskReport.set(ZendeskConstants.Field.START_TIME, 1557026576);

        when(zendeskSupportAPIService.execute(anyInt(), any(Schema.class), any(PageBuilder.class))).thenReturn(taskReport);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-05-05 03:22:56 +0000", nextStartTime);
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
    public void testDispatchPerTargetShouldReturnUserEventService()
    {
        // create a new one so it doesn't mock the ZendeskService
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", Target.USER_EVENTS.name().toLowerCase());
        src.set("profile_source", "dummy");
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskUserEventService);
    }

    private void testReturnSupportAPIService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskSupportAPIService);
    }

    private void testReturnNPSService(Target target)
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", target.name().toLowerCase());
        src.set("columns", Collections.EMPTY_LIST);
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
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
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskService zendeskService = zendeskInputPlugin.dispatchPerTarget(task);
        assertTrue(zendeskService instanceof ZendeskCustomObjectService);
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJson);
    }

    private void setupTestGuessGenerateColumn(ConfigSource src, String fileName, String expectedSource)
    {
        loadData(fileName);
        ConfigDiff configDiff = zendeskInputPlugin.guess(src);
        JsonNode columns = configDiff.get(JsonNode.class, "columns");
        assertEquals(ZendeskTestHelper.getJsonFromFile(expectedSource), columns);
    }

    private void setupSupportAPIService()
    {
        zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);
        doReturn(zendeskSupportAPIService).when(zendeskInputPlugin).dispatchPerTarget(any(ZendeskInputPlugin.PluginTask.class));
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
