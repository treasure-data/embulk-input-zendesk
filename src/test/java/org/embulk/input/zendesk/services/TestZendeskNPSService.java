package org.embulk.input.zendesk.services;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import static org.mockito.Mockito.when;

public class TestZendeskNPSService
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ZendeskRestClient zendeskRestClient;

    private ZendeskNPSService zendeskNPSService;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
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
