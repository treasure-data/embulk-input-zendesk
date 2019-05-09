package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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

    private Schema schema = mock(Schema.class);

    private PageBuilder pageBuilder = mock(PageBuilder.class);

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

    @Test
    public void testRunIncremental()
    {
        setup();
        loadData();

        TaskReport taskReport = zendeskNPSService.execute(0, schema, pageBuilder);
        verify(pageBuilder, times(1)).addRecord();
        Assert.assertFalse(taskReport.isEmpty());
        Assert.assertEquals(1547968494, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    private void loadData()
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/scores.json");
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
