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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskChatService {
    @Rule public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskChatService zendeskChatService;

    private RecordImporter recordImporter;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
        recordImporter = mock(RecordImporter.class);
    }

    @Test
    public void testGetListPathForObjectRecord()
    {
        setup();
        loadData();

        TaskReport taskReport = zendeskChatService.addRecordToImporter(0, recordImporter);
        assertEquals(1569733201, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
        assertEquals(1602478801, taskReport.get(JsonNode.class, ZendeskConstants.Field.END_TIME).asLong());
    }

    @Test
    public void testFetchData()
    {
        setup();
        JsonNode dataSearchJson = ZendeskTestHelper.getJsonFromFile("data/chat_search.json");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/chat.json");

        when(zendeskRestClient.doGet(eq("https://www.zopim.com/api/v2/chats/search?q=timestamp%3A%5B2018-09-15T05%3A00%3A00Z+TO+2019-09-29T05%3A00%3A00Z%5D&page=1"), any(), anyBoolean())).thenReturn(dataSearchJson.toString());

        when(zendeskRestClient.doGet(eq("https://www.zopim.com/api/v2/chats?ids=id_1%2Cid_2"), any(), anyBoolean())).thenReturn(dataJson.toString());

        zendeskChatService.fetchData("2018-09-15T05:00:00Z", "2019-09-29T05:00:00Z", 1, recordImporter);

        verify(recordImporter, times(2)).addRecord(any());
    }

    private void setup()
    {
        ZendeskInputPlugin.PluginTask task = ZendeskTestHelper.getConfigSource("chat.yml").loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskChatService(task);
    }

    private void setupZendeskChatService(ZendeskInputPlugin.PluginTask task)
    {
        zendeskChatService = spy(new ZendeskChatService(task));
        when(zendeskChatService.getZendeskRestClient()).thenReturn(zendeskRestClient);
    }

    private void loadData()
    {
        JsonNode dataSearchJson = ZendeskTestHelper.getJsonFromFile("data/chat_search.json");
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/chat.json");

        when(zendeskRestClient.doGet(any(), any(), anyBoolean())).thenReturn(dataSearchJson.toString()).thenReturn(dataJson.toString());
    }
}
