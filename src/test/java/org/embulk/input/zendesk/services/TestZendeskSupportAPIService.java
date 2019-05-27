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
    public void testBuildPathWithIncrementalForPreview()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        setup("incremental.yml");
        loadData("data/tickets.json");
        setupTestAndVerifyURL(expectURL, 0, true);
    }

    @Test
    public void testBuildPathWithNonIncrementalForPreview()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=0";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        setupTestAndVerifyURL(expectURL, 0, true);
    }

    @Test
    public void testBuildPathWithIncrementalForPreviewWithTicketMetrics()
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
    public void testBuildPathWithIncrementalForRun()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        setup("incremental.yml");
        loadData("data/tickets.json");
        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void testBuildPathWithIncrementalForRunWithTicketMetrics()
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
    public void testBuildPathWithIncrementalForRunIncludeRelatedObject()
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
    public void testBuildPathWithIncrementalForRunTimeChange()
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
    public void testBuildPathWithNonIncrementalForRun()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=0";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");

        setupTestAndVerifyURL(expectURL, 0, false);
    }

    @Test
    public void testBuildPathWithNonIncrementalForRunChangePageNumber()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=2";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        setupTestAndVerifyURL(expectURL, 2, false);
    }

    @Test
    public void testAddRecordToImporterWithIncremental()
    {
        setup("incremental.yml");
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
        setup("incremental.yml");
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
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");

        TaskReport taskReport = zendeskSupportAPIService.addRecordToImporter(0, recordImporter);
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
