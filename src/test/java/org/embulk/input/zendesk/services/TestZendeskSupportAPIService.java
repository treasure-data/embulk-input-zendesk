package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

public class TestZendeskSupportAPIService
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskSupportAPIService zendeskSupportAPIService;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
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
}
