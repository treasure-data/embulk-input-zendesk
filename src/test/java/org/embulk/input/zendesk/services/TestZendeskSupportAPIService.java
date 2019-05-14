package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;

public class TestZendeskSupportAPIService
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskSupportAPIService zendeskSupportAPIService;

    private RecordImporter recordImporter;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
        recordImporter = mock(RecordImporter.class);
    }

    @Test
    public void buildPathWithIncrementalForPreview()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        setup("incremental.yml");
        loadData("data/tickets.json");
        setupTestAndVerifyURL(expectURL, 0, true);
    }

    @Test
    public void buildPathWithNonIncrementalForPreview()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=0";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        setupTestAndVerifyURL(expectURL, 0, true);
    }

    @Test
    public void buildPathWithIncrementalForPreviewWithTicketMetrics()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0&include=metric_sets";
        loadData("data/ticket_metrics.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", "ticket_metrics");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        setupTestAndVerifyURL(expectURL, 0, true);
    }

    @Test
    public void buildPathWithIncrementalForRun()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        setup("incremental.yml");
        loadData("data/tickets.json");
        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void buildPathWithIncrementalForRunWithTicketMetrics()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0&include=metric_sets";
        loadData("data/ticket_metrics.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", "ticket_metrics");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void buildPathWithIncrementalForRunIncludeRelatedObject()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        loadData("data/tickets.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("includes", Collections.singletonList("organizations"));
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void buildPathWithIncrementalForRunTimeChange()
    {
        long time = 100;
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=100";
        loadData("data/tickets.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        zendeskSupportAPIService.getData("", 0, false, time);
        final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient).doGet(url.capture(), any(), anyBoolean());
        assertEquals(expectURL, url.getValue());
    }

    @Test
    public void buildPathWithNonIncrementalForRun()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=0";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");

        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void buildPathWithNonIncrementalForRunChangePageNumber()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=2";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        setupTestAndVerifyURL(expectURL, 2, false);
    }

    @Test
    public void executeIncremental()
    {
        setup("incremental.yml");
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(4)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalWithoutDedup()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("dedup", false);
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(5)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalContainUpdatedBySystemRecord()
    {
        setup("incremental.yml");
        loadData("data/ticket_with_updated_by_system_records.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(3)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalContainEndTime()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        // same updated_at time of last record
        src.set("end_time", "2019-02-20T07:17:32Z");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(3)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        // start_time = end_time + 1
        Assert.assertEquals(1550647053, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalContainEndTimeFilterOutLastRecord()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        // earlier than updated_at time of last record
        src.set("end_time", "2019-02-20T07:17:32Z");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(3)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        //
        Assert.assertEquals(1550647053, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalContainEndTimeFilterOutLastRecordTicketEvents()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", Target.TICKET_EVENTS.toString());
        // earlier than updated_at time of last record
        // 1550645520
        src.set("end_time", "2019-02-20T06:52:00Z");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/ticket_events_updated_by_system_records.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(3)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        // end_time + 1
        Assert.assertEquals(1550645521, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void executeIncrementalUpdateStartTimeWhenEmptyResult()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", Target.TICKETS.toString());

        src.set("start_time", "2219-02-20T06:52:00Z");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/empty_result.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertTrue(Instant.now().getEpochSecond() <= taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong() + 50);
    }

    @Test
    public void executeNonIncremental()
    {
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");

        TaskReport taskReport = zendeskSupportAPIService.execute(0, recordImporter);
        verify(recordImporter, times(7)).addRecord(any());
        Assert.assertTrue(taskReport.isEmpty());
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskRestClient.doGet(any(), any(), anyBoolean())).thenReturn(dataJson.toString());
    }

    private void setupZendeskSupportAPIService(ZendeskInputPlugin.PluginTask task)
    {
        zendeskSupportAPIService = spy(new ZendeskSupportAPIService(task));
        when(zendeskSupportAPIService.getZendeskRestClient()).thenReturn(zendeskRestClient);
    }

    private void setup(String file)
    {
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource(file)
                .loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
    }

    private void setupTestAndVerifyURL(String expectURL, int page, boolean isPreview)
    {
        zendeskSupportAPIService.getData("", page, isPreview, 0);
        final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient).doGet(url.capture(), any(), anyBoolean());
        assertEquals(expectURL, url.getValue());
    }
}
