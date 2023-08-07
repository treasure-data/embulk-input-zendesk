package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.EmbulkTestRuntime;

import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.embulk.input.zendesk.ZendeskInputPlugin.CONFIG_MAPPER;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskCursorBasedService
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskCursorBasedService zendeskCursorBasedService;

    private RecordImporter recordImporter;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
        recordImporter = mock(RecordImporter.class);
    }

    @Test
    public void testRunNonIncremental()
    {
        setup("incremental.yml");
        loadData("data/cursor_based_tickets.json");

        String expectedString = "https://abc.zendesk.com/api/v2/incremental/tickets/cursor.json?start_time=1547275910";

        zendeskCursorBasedService.addRecordToImporter(0, recordImporter);
        final ArgumentCaptor<String> actualString = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(1)).doGet(actualString.capture(), any(), anyBoolean());
        assertTrue(actualString.getAllValues().contains(expectedString));

        verify(recordImporter, times(1)).addRecord(any());
    }

    @Test
    public void testRunIncremental()
    {
        setup("incremental.yml");
        loadData("data/cursor_based_tickets_incremental.json", "data/cursor_based_tickets.json");

        String expectedString = "https://abc.zendesk.com/api/v2/incremental/tickets/cursor.json?start_time=1547275910";
        String expectedNextString = "https://treasuredata.zendesk.com/api/v2/incremental/tickets/cursor.json?cursor=xxxx";

        TaskReport taskReport = zendeskCursorBasedService.addRecordToImporter(0, recordImporter);
        final ArgumentCaptor<String> actualString = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(2)).doGet(actualString.capture(), any(), anyBoolean());
        assertTrue(actualString.getAllValues().get(0).contains(expectedString));
        assertTrue(actualString.getAllValues().get(1).contains(expectedNextString));

        verify(recordImporter, times(2)).addRecord(any());
        assertTrue(taskReport.get(String.class, ZendeskConstants.Field.START_TIME).equals("1437638600"));
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskRestClient.doGet(any(), any(), anyBoolean())).thenReturn(dataJson.toString());
    }

    private void loadData(String fileName, String nextFile)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        JsonNode dataJsonNextFile = ZendeskTestHelper.getJsonFromFile(nextFile);

        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
            .thenReturn(dataJson.toString())
            .thenReturn(dataJsonNextFile.toString());
    }

    private void setupZendeskSupportAPIService(ZendeskInputPlugin.PluginTask task)
    {
        zendeskCursorBasedService = spy(new ZendeskCursorBasedService(task));
        when(zendeskCursorBasedService.getZendeskRestClient()).thenReturn(zendeskRestClient);
    }

    private void setup(String file)
    {
        ZendeskInputPlugin.PluginTask task =
            CONFIG_MAPPER.map(ZendeskTestHelper.getConfigSource(file), ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
    }
}
