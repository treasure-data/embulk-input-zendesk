package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
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

public class TestZendeskNPSService
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskNPSService zendeskNPSService;

    private RecordImporter recordImporter;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
        recordImporter = mock(RecordImporter.class);
    }

    @Test
    public void testBuildURL()
    {
        setup();
        String expectedString = "https://abc.zendesk.com/api/v2/nps/incremental/responses.json?start_time=10000";
        // only use start_time so page any value
        String actualString = zendeskNPSService.buildURI(0, 10000);
        assertEquals(expectedString, actualString);
    }

    @Test
    public void testAddRecordToImporterIncremental()
    {
        setup();
        loadData("data/scores.json");

        TaskReport taskReport = zendeskNPSService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(1)).addRecord(any());
        Assert.assertEquals(1555418871, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testAddRecordToImporterIncrementalWithNextPageAndAllRecordsShareTheSameTime()
    {
        // api_end_time of ticket_share_same_time_with_next_page.json
        String expectedURL = "https://abc.zendesk.com/api/v2/nps/incremental/responses.json?start_time=1555418820";

        setup();
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/scores_share_same_time_with_next_page.json");
        JsonNode dataJsonNext = ZendeskTestHelper.getJsonFromFile("data/scores.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJson.toString())
                .thenReturn(dataJsonNext.toString());

        TaskReport taskReport = zendeskNPSService.addRecordToImporter(0, recordImporter);
        final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(2)).doGet(url.capture(), any(), anyBoolean());
        assertEquals(expectedURL, url.getValue());

        verify(recordImporter, times(3)).addRecord(any());
        // api_end_time of scores.json + 1
        Assert.assertEquals(1555418871, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testAddRecordToImporterIncrementaAndAllRecordsShareTheSameTime()
    {
        setup();
        loadData("data/scores_share_same_time_without_next_page.json");

        TaskReport taskReport = zendeskNPSService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(2)).addRecord(any());
        // api_end_time of scores_share_same_time_without_next_page.json + 1
        Assert.assertEquals(1555418821, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskRestClient.doGet(any(), any(), anyBoolean())).thenReturn(dataJson.toString());
    }

    private void setupZendeskNPSService(ZendeskInputPlugin.PluginTask task)
    {
        zendeskNPSService = spy(new ZendeskNPSService(task));
        when(zendeskNPSService.getZendeskRestClient()).thenReturn(zendeskRestClient);
    }

    private void setup()
    {
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource("nps.yml")
                .loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskNPSService(task);
    }
}
