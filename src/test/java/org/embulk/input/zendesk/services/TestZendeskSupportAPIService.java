package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;

public class TestZendeskSupportAPIService
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskSupportAPIService zendeskSupportAPIService;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
    }

    @Test
    public void testBuildPathWithIncremental()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        setup("incremental.yml");
        loadData("data/tickets.json");
        String url = zendeskSupportAPIService.buildURI(0, 0);
        assertEquals(expectURL, url);
    }

    @Test
    public void testBuildPathWithIncrementalWithTicketMetrics()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0&include=metric_sets";
        loadData("data/ticket_metrics.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("target", "ticket_metrics");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        String url = zendeskSupportAPIService.buildURI(0, 0);
        assertEquals(expectURL, url);
    }

    @Test
    public void testBuildPathWithIncrementalIncludeRelatedObject()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=0";
        loadData("data/tickets.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        src.set("includes", Collections.singletonList("organizations"));
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);

        String url = zendeskSupportAPIService.buildURI(0, 0);
        assertEquals(expectURL, url);
    }

    @Test
    public void testBuildPathWithIncrementalTimeChange()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/incremental/tickets.json?start_time=100";
        loadData("data/tickets.json");

        ConfigSource src = ZendeskTestHelper.getConfigSource("incremental.yml");
        ZendeskInputPlugin.PluginTask task = src.loadConfig(ZendeskInputPlugin.PluginTask.class);
        setupZendeskSupportAPIService(task);
        String url = zendeskSupportAPIService.buildURI(0, 100);
        assertEquals(expectURL, url);
    }

    @Test
    public void testBuildPathWithNonIncremental()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=0";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        String url = zendeskSupportAPIService.buildURI(0, 0);
        assertEquals(expectURL, url);
    }

    @Test
    public void testBuildPathWithNonIncrementalChangePageNumber()
    {
        String expectURL = "https://abc.zendesk.com/api/v2/ticket_fields.json?sort_by=id&per_page=100&page=2";
        setup("non-incremental.yml");
        loadData("data/ticket_fields.json");
        String url = zendeskSupportAPIService.buildURI(2, 0);
        assertEquals(expectURL, url);
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
}
