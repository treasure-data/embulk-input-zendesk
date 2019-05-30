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

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskNormalService
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
    public void testAddRecordToImporterWithIncremental()
    {
        setupSupportAPIService("incremental.yml");
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(4)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testAddRecordToImporterWithIncrementalAndWithoutDedup()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("dedup", false);
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/tickets.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(5)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testAddRecordToImporterIncrementalContainUpdatedBySystemRecords()
    {
        setupSupportAPIService("incremental.yml");
        loadData("data/ticket_with_updated_by_system_records.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(3)).addRecord(any());
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1550647054, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testAddRecordToImporterIncrementalUpdateStartTimeWhenEmptyResult()
    {
        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", Target.TICKETS.toString());

        src.set("start_time", "2219-02-20T06:52:00Z");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/empty_result.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertTrue(Instant.now().getEpochSecond() <= taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong() + 50);
    }

    @Test
    public void testAddRecordToImporterNonIncremental()
    {
        setupSupportAPIService("non-incremental.yml");
        loadData("data/ticket_fields.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(7)).addRecord(any());
        Assert.assertTrue(taskReport.isEmpty());
    }

    @Test
    public void testAddRecordToImporterIncrementalForSupportAndAllRecordsShareTheSameTime()
    {
        setupSupportAPIService("incremental.yml");
        loadData("data/ticket_share_same_time_without_next_page.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(4)).addRecord(any());
        // api_end_time of ticket_share_same_time_without_next_page.json + 1
        Assert.assertEquals(1551419520, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testTicketEventsAddRecordToImporterIncrementalWithNextPageAndAllRecordsShareTheSameTime()
    {
        // api_end_time of ticket_events_share_same_time_with_next_page.json
        String expectedURL = "https://abc.zendesk.com/api/v2/incremental/ticket_events.json?start_time=1550645443";
        setupSupportAPIService("incremental.yml");
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource("incremental.yml")
                .set("target", "ticket_events")
                .loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/ticket_events_share_same_time_with_next_page.json");
        JsonNode dataJsonNext = ZendeskTestHelper.getJsonFromFile("data/ticket_events_updated_by_system_records.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJson.toString())
                .thenReturn(dataJsonNext.toString());

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(2)).doGet(url.capture(), any(), anyBoolean());
        assertEquals(expectedURL, url.getValue());

        verify(recordImporter, times(4)).addRecord(any());
        // api_end_time of ticket_events_updated_by_system_records.json + 1
        Assert.assertEquals(1550645523, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testTicketEventsAddRecordToImporterIncrementaAndAllRecordsShareTheSameTime()
    {
        setupSupportAPIService("incremental.yml");
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource("incremental.yml")
                .set("target", "ticket_events")
                .loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        loadData("data/ticket_events_share_same_time_without_next_page.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(4)).addRecord(any());
        // api_end_time of ticket_events_share_same_time_without_next_page.json + 1
        Assert.assertEquals(1550645444, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    private void setupSupportAPIService(String file)
    {
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource(file)
                .loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
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
}
