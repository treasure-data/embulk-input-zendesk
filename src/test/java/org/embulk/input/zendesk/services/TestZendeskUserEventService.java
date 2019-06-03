package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskUserEventService
{
    private RecordImporter recordImporter;

    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ZendeskRestClient zendeskRestClient;
    private ZendeskUserEventService zendeskUserEventService;

    @Before
    public void prepare()
    {
        zendeskRestClient = mock(ZendeskRestClient.class);
        recordImporter = mock(RecordImporter.class);
    }

    @Test
    public void testGetData()
    {
        setup();

        JsonNode jsonNode = zendeskUserEventService.getDataFromPath("https://abc.zendesk.com/api/sunshine/objects/records?type=user&per_page=1000", 0, true, 0);
        assertFalse(jsonNode.isNull());
        assertTrue(jsonNode.has("data"));
        assertTrue(jsonNode.get("data").isArray());
    }

    @Test
    public void testAddRecordToImporterInPreviewMode()
    {
        setup();
        ZendeskTestHelper.setPreviewMode(runtime, true);
        zendeskUserEventService.addRecordToImporter(0, recordImporter);
        verify(recordImporter, times(1)).addRecord(any());
    }

    @Test
    public void testUrlForUserEvent()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        setup();
        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        JsonNode dataJsonUserEventWithLatterTime = ZendeskTestHelper.getJsonFromFile("data/user_event_contain_latter_create_at.json");

        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEventWithLatterTime.toString());

        String expectedURIForOrganization = "https://abc.zendesk.com/api/v2/organizations?per_page=100&page=1";
        String expectedURIForUser = "https://abc.zendesk.com/api/v2/organizations/360857467053/users.json?per_page=100&page=1";
        String expectedURIForUserEvent = "https://abc.zendesk.com/api/sunshine/events?identifier=support%3Auser_id%3A1194092277&start_time=2019-01-20T07%3A14%3A50Z&end_time=2019-06-20T07%3A14%3A53Z";
        List<String> expectedURI = Arrays.asList(expectedURIForOrganization, expectedURIForUser, expectedURIForUserEvent);

        zendeskUserEventService.addRecordToImporter(0, recordImporter);
        ArgumentCaptor<String> uri = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(3)).doGet(uri.capture(), any(), anyBoolean());
        assertEquals(expectedURI, uri.getAllValues());
    }

    @Test
    public void testAddRecordToImporterInNonPreviewMode()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        PluginTask task = setup();

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event.json");

        // 2 organizations but return the same user.
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonOrganization.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations/360857467053/users.json?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonUser.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations/360857467055/users.json?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonUser.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/sunshine/events?identifier=support%3Auser_id%3A1194092277&start_time=2019-01-20T07%3A14%3A50Z&end_time=2019-06-20T07%3A14%3A53Z"), eq(task), eq(false)))
                .thenReturn(dataJsonUserEvent.toString());

        zendeskUserEventService.addRecordToImporter(0, recordImporter);
        // non dedup
        verify(recordImporter, times(2)).addRecord(any());
    }

    @Test
    public void testAddRecordToImporterWithDuplicateUser()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        PluginTask task = ZendeskTestHelper.getConfigSource("user_events.yml")
                .set("dedup", true)
                .loadConfig(PluginTask.class);
        setupZendeskSupportAPIService(task);

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event.json");
        // 2 organizations but return the same user.
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonOrganization.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations/360857467053/users.json?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonUser.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/v2/organizations/360857467055/users.json?per_page=100&page=1"), eq(task), eq(false)))
                .thenReturn(dataJsonUser.toString());
        when(zendeskRestClient.doGet(eq("https://abc.zendesk.com/api/sunshine/events?identifier=support%3Auser_id%3A1194092277&start_time=2019-01-20T07%3A14%3A50Z&end_time=2019-06-20T07%3A14%3A53Z"), eq(task), eq(false)))
                .thenReturn(dataJsonUserEvent.toString());

        zendeskUserEventService.addRecordToImporter(0, recordImporter);

        // expected to call fetchUserEvent only one time
        verify(recordImporter, times(1)).addRecord(any());
    }

    private void setupZendeskSupportAPIService(PluginTask task)
    {
        zendeskUserEventService = spy(new ZendeskUserEventService(task));
        when(zendeskUserEventService.getZendeskRestClient()).thenReturn(zendeskRestClient);
    }

    private PluginTask setup()
    {
        PluginTask task = ZendeskTestHelper.getConfigSource("user_events.yml")
                .loadConfig(PluginTask.class);
        setupZendeskSupportAPIService(task);
        return task;
    }
}
