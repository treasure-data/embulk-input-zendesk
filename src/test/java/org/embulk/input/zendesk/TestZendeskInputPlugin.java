package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.input.zendesk.services.ZendeskService;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.utils.ZendeskPluginTestRuntime;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import static org.mockito.Mockito.when;

import java.util.Collections;

public class TestZendeskInputPlugin
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public ZendeskPluginTestRuntime embulk = new ZendeskPluginTestRuntime();

    private ZendeskService zendeskSupportAPIService;

    private ZendeskInputPlugin zendeskInputPlugin;

    private PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);

    @Before
    public void prepare()
    {
        zendeskInputPlugin = spy(new ZendeskInputPlugin());
        setupSupportAPIService();
        doReturn(pageBuilder).when(zendeskInputPlugin).getPageBuilder(any(Schema.class), any(PageOutput.class));
    }

    @Test
    public void testGuessGenerateColumnsForIncrementalTarget()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        setupTestGuessGenerateColumn(src, "data/tickets.json", "data/expected/ticket_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForIncrementalTargetIncludeRelatedObject()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        src.set("includes", Collections.singletonList("organizations"));
        setupTestGuessGenerateColumn(src, "data/tickets.json", "data/expected/ticket_column_with_related_objects.json");
    }

    @Test
    public void testGuessGenerateColumnsForTicketMetrics()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "ticket_metrics");
        setupTestGuessGenerateColumn(src, "data/ticket_metrics.json", "data/expected/ticket_metrics_column.json");
    }

    @Test
    public void testGuessGenerateColumnsForNonIncrementalTarget()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "ticket_fields");
        setupTestGuessGenerateColumn(src, "data/ticket_fields.json", "data/expected/ticket_fields_column.json");
    }

    @Test(expected = ConfigException.class)
    public void testGuessFail()
    {
        final ConfigSource src = ZendeskTestHelper.getConfigSource("base.yml");
        src.set("target", "tickets");
        loadData("data/error_data.json");

        zendeskInputPlugin.guess(src);
    }

    private void loadData(String fileName)
    {
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile(fileName);
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean(), anyLong())).thenReturn(dataJson);
    }

    private void setupTestGuessGenerateColumn(ConfigSource src, String fileName, String expectedSource)
    {
        loadData(fileName);
        ConfigDiff configDiff = zendeskInputPlugin.guess(src);
        JsonNode columns = configDiff.get(JsonNode.class, "columns");
        assertEquals(ZendeskTestHelper.getJsonFromFile(expectedSource), columns);
    }

    private void setupSupportAPIService()
    {
        zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);
        doReturn(zendeskSupportAPIService).when(zendeskInputPlugin).dispatchPerTarget(any(ZendeskInputPlugin.PluginTask.class));
    }
}
