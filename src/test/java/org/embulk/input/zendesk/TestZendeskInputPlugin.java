package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
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
import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TestZendeskInputPlugin
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public ZendeskPluginTestRuntime embulk = new ZendeskPluginTestRuntime();

    private ZendeskSupportAPIService zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);

    private ZendeskInputPlugin zendeskInputPlugin = spy(new ZendeskInputPlugin());

    private TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();

    private PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);

    @Before
    public void prepare()
    {
        when(zendeskInputPlugin.getZendeskSupportAPIService(any(ZendeskInputPlugin.PluginTask.class))).thenReturn(zendeskSupportAPIService);
        doReturn(pageBuilder).when(zendeskInputPlugin).getPageBuilder(any(Schema.class), any(PageOutput.class));
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean())).thenReturn(dataJson);
    }

    private void setupTestGuessGenerateColumn(ConfigSource src, String fileName, String expectedSource)
    {
        loadData(fileName);
        ConfigDiff configDiff = zendeskInputPlugin.guess(src);
        JsonNode columns = configDiff.get(JsonNode.class, "columns");
        assertEquals(ZendeskTestHelper.getJsonFromFile(expectedSource), columns);
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
    public void testRunIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");

        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean())).thenReturn(dataJson);

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        String nextStartTime = configDiff.get(String.class, "start_time");
        verify(pageBuilder, times(4)).addRecord();
        verify(pageBuilder, times(1)).finish();
        assertEquals("2019-02-20 07:17:33 +0000", nextStartTime);
    }

    @Test
    public void testRunIncrementalWithRelatedObject()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("includes", Collections.singletonList("organizations"));

        loadData("data/tickets.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        verify(zendeskInputPlugin, times(4))
                .dedupAndFetchData(any(ZendeskInputPlugin.PluginTask.class), any(JsonNode.class), anyBoolean(), anyLong(),
                        any(ImmutableList.Builder.class), anyList(), any(Schema.class), any(PageBuilder.class));
        verify(pageBuilder, times(1)).finish();
        String nextStartTime = configDiff.get(String.class, "start_time");
        assertEquals("2019-02-20 07:17:33 +0000", nextStartTime);
    }

    @Test
    public void testRunNonIncremental()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("non-incremental.yml");
        loadData("data/ticket_fields.json");

        ConfigDiff configDiff = zendeskInputPlugin.transaction(src, new Control());
        verify(pageBuilder, times(7)).addRecord();
        verify(pageBuilder, times(1)).finish();
        Assert.assertTrue(configDiff.isEmpty());
    }

    @Test
    public void testDedupAndFetchDataShouldUpdateRequiredToCheckRecord()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        final ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        Schema schema = task.getColumns().toSchema();

        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean())).thenReturn(dataJson);

        ImmutableList.Builder requiredToCheckedRecords = new ImmutableList.Builder();
        List<String> previousRecordList = new ArrayList<>();

        zendeskInputPlugin.dedupAndFetchData(task, dataJson.get("tickets").get(0), true, 1550647053, requiredToCheckedRecords,
                previousRecordList, schema, pageBuilder);
        Assert.assertTrue(requiredToCheckedRecords.build().isEmpty());

        zendeskInputPlugin.dedupAndFetchData(task, dataJson.get("tickets").get(3), true, 1550647053, requiredToCheckedRecords,
                previousRecordList, schema, pageBuilder);
        Assert.assertTrue(requiredToCheckedRecords.build().contains("1002"));

        verify(pageBuilder, times(2)).addRecord();
    }

    @Test
    public void testDedupAndFetchDataShouldStop()
    {
        // updated_at of first record
        String startTime = "2019-02-20 06:51:50 +0000";
        long time = ZendeskDateUtils.isoToEpochSecond(startTime);

        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("start_time", startTime);

        final ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        Schema schema = task.getColumns().toSchema();

        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean())).thenReturn(dataJson);

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        List<String> previousRecordList = new ArrayList<>();

        JsonNode record = dataJson.get("tickets").get(0);

        // equal time do nothing
        zendeskInputPlugin.dedupAndFetchData(task, record, true, time, builder,
                previousRecordList, schema, pageBuilder);
        verify(zendeskInputPlugin, times(1))
                .isRequiredForFurtherChecking(Optional.of(startTime),
                        ZendeskDateUtils.isoToEpochSecond(record.get("updated_at").asText()));

        // larger than time stop
        record = dataJson.get("tickets").get(1);
        zendeskInputPlugin.dedupAndFetchData(task, record, true, time, builder,
                previousRecordList, schema, pageBuilder);
        verify(zendeskInputPlugin, times(1)).isRequiredForFurtherChecking(Optional.of(startTime),
                        ZendeskDateUtils.isoToEpochSecond(record.get("updated_at").asText()));

        // Do not need for further checking
        record = dataJson.get("tickets").get(3);
        zendeskInputPlugin.dedupAndFetchData(task, record, true, time, builder,
                previousRecordList, schema, pageBuilder);
        verify(zendeskInputPlugin, times(0)).isRequiredForFurtherChecking(Optional.of(startTime),
                        ZendeskDateUtils.isoToEpochSecond(record.get("updated_at").asText()));

        verify(pageBuilder, times(3)).addRecord();
    }

    @Test
    public void testDedupAndFetchDataDoNotAddDuplicatedRecord()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("includes", Collections.singletonList("organizations"));
        final ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        Schema schema = task.getColumns().toSchema();

        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        JsonNode singleObjectJson = ZendeskTestHelper.getJsonFromFile("data/ticket_with_related_objects.json");

        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean()))
                .thenReturn(singleObjectJson);

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        List<String> previousRecordList = new ArrayList<>();

        // time of the last record
        long time = ZendeskDateUtils.isoToEpochSecond(dataJson.get("tickets").get(3).get("updated_at").asText());
        String expectedId = dataJson.get("tickets").get(3).get("id").toString();

        zendeskInputPlugin.dedupAndFetchData(task, dataJson.get("tickets").get(0), true, time, builder,
                previousRecordList, schema, pageBuilder);
        verify(pageBuilder, times(1)).addRecord();

        // duplicated and don't add
        previousRecordList.add(expectedId);
        zendeskInputPlugin.dedupAndFetchData(task, dataJson.get("tickets").get(3), true, time, builder,
                previousRecordList, schema, pageBuilder);
        verify(pageBuilder, times(1)).addRecord();
        Assert.assertTrue(builder.build().contains("1002"));
    }

    private class Control implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(final TaskSource taskSource, final Schema schema, final int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(zendeskInputPlugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }
}
