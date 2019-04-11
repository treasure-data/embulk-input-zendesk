package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
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
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

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

    private ZendeskSupportAPIService zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);

    private ZendeskInputPlugin zendeskInputPlugin;

    private TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();

    private PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);

    @Before
    public void prepare()
    {
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        when(zendeskInputPlugin.getZendeskSupportAPIService(any(ZendeskInputPlugin.PluginTask.class))).thenReturn(zendeskSupportAPIService);
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
    public void testRunIncrementalDedup()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");

        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJson);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        verify(pageBuilder, times(4)).addRecord();
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-02-20 07:17:34 +0000", nextStartTime);
    }

    @Test
    public void testRunIncrementalWithNextPage()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets_continue.json");
        JsonNode dataJsonNext = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong()))
                .thenReturn(dataJson)
                .thenReturn(dataJsonNext);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        verify(pageBuilder, times(1)).addRecord();
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-02-20 07:17:34 +0000", nextStartTime);
    }

    @Test
    public void testRunIncrementalNonDedup()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("dedup", false);

        JsonNode dataJsonNext = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJsonNext);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        verify(pageBuilder, times(5)).addRecord();
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-02-20 07:17:34 +0000", nextStartTime);
    }

    @Test
    public void testRunIncrementalForTicketMetrics()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", "ticket_metrics");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/ticket_metrics.json");

        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJson);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        verify(pageBuilder, times(3)).addRecord();
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-02-20 07:17:34 +0000", nextStartTime);
    }

    @Test
    public void testRunIncrementalWithRelatedObject()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("includes", Collections.singletonList("organizations"));

        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        JsonNode dataJsonObject = ZendeskTestHelper.getJsonFromFile("data/ticket_with_related_objects.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong()))
                .thenReturn(dataJson)
                .thenReturn(dataJsonObject);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());

        verify(pageBuilder, times(1)).finish();
        String nextStartTime = configDiff.get(String.class, ZendeskConstants.Field.START_TIME);
        assertEquals("2019-02-20 07:17:34 +0000", nextStartTime);
    }

    @Test
    public void testRunNonIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("non-incremental.yml");
        loadData("data/ticket_fields.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        // running in 2 pages
        verify(pageBuilder, times(7 * 2)).addRecord();
        verify(pageBuilder, times(2)).finish();
        Assert.assertTrue(configDiff.isEmpty());
    }

    @Test
    public void testRunNotGenerateStartTimeWhenRunningInNonIncrementalModeAndTargetSupportIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("incremental", false);
        loadData("data/tickets.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());

        Assert.assertTrue(configDiff.isEmpty());
    }

    @Test
    public void testRunNotGenerateStartTimeWhenRunningInIncrementalModeAndTargetDoesNotSupportIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("non-incremental.yml");
        src.set("incremental", true);
        loadData("data/ticket_fields.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());

        Assert.assertTrue(configDiff.isEmpty());
    }

    @Test
    public void testRunIncrementalGenerateStartTimeWhenRunningInIncrementalModeAndTargetSupportIncremental()
    {
        String expectedStartTime = "2019-02-20 07:17:34 +0000";
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        loadData("data/tickets.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        Assert.assertFalse(configDiff.isEmpty());
        Assert.assertEquals(expectedStartTime, configDiff.get(String.class, ZendeskConstants.Field.START_TIME));
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
}
