package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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

    private TaskReport taskReport = mock(TaskReport.class);

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
    public void testGuess()
    {
        setup();

        JsonNode jsonNode = zendeskUserEventService.getData("https://abc.zendesk.com/api/sunshine/objects/records?type=user&per_page=1000", 0, true, 0);
        assertFalse(jsonNode.isNull());
        assertTrue(jsonNode.has("data"));
        assertTrue(jsonNode.get("data").isArray());
    }

    @Test
    public void testPreview()
    {
        setup();
        ZendeskTestHelper.setPreviewMode(runtime, true);
        zendeskUserEventService.execute(0, recordImporter);
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

        zendeskUserEventService.execute(0, recordImporter);
        ArgumentCaptor<String> uri = ArgumentCaptor.forClass(String.class);
        verify(zendeskRestClient, times(3)).doGet(uri.capture(), any(), anyBoolean());
        assertEquals(expectedURI, uri.getAllValues());
    }

    @Test
    public void testRun()
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

        zendeskUserEventService.execute(0, recordImporter);
        // non dedup
        verify(recordImporter, times(2)).addRecord(any());
        assertFalse(taskReport.isEmpty());
    }

    @Test
    public void testRunWithDuplicateUser()
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

        zendeskUserEventService.execute(0, recordImporter);

        // expected to call fetchUserEvent only one time
        verify(recordImporter, times(1)).addRecord(any());
    }

    @Test
    public void testRunContainSomeRecordsLatterThanEndTime()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        PluginTask task = ZendeskTestHelper.getConfigSource("user_events.yml")
                .set("end_time", "2019-03-06T02:34:25Z")
                .loadConfig(PluginTask.class);
        setupZendeskSupportAPIService(task);

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        // contain 2 records but 1 latter records
        JsonNode dataJsonUserEventWithLatterTime = ZendeskTestHelper.getJsonFromFile("data/user_event_multiple.json");

        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEventWithLatterTime.toString());

        zendeskUserEventService.execute(0, recordImporter);
        // one record to add
        verify(recordImporter, times(1)).addRecord(any());
    }

    @Test
    public void testNextStartTimeWhenImportRecord()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        long expectedNextStartTime = 1551839663;
        setup();

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEvent.toString());

        TaskReport taskReport = zendeskUserEventService.execute(0, recordImporter);
        assertEquals(expectedNextStartTime, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testNextStartTimeWhenNoRecord()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        long expectedNextStartTime = 1551839663;
        setup();

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event_contain_latter_create_at.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEvent.toString());

        TaskReport taskReport = zendeskUserEventService.execute(0, recordImporter);
        // Start_time will be now
        assertNotEquals(expectedNextStartTime, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testNextStartTimeWithEndTime()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        long expectedNextStartTime = 1551839663;
        PluginTask task = ZendeskTestHelper.getConfigSource("user_events.yml")
                .set("end_time", "2019-03-06T02:34:25Z")
                .loadConfig(PluginTask.class);
        setupZendeskSupportAPIService(task);

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        // contain 2 records but 1 later records
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event_multiple.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEvent.toString());

        TaskReport taskReport = zendeskUserEventService.execute(0, recordImporter);
        // Next start time will be the latter imported records time
        assertEquals(expectedNextStartTime, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
    }

    @Test
    public void testNextEndTime()
    {
        ZendeskTestHelper.setPreviewMode(runtime, false);
        long expectedNextStartTime = 1551839663;
        PluginTask task = ZendeskTestHelper.getConfigSource("user_events.yml")
                .set("end_time", "2019-03-06T02:34:25Z")
                .loadConfig(PluginTask.class);
        setupZendeskSupportAPIService(task);

        JsonNode dataJsonOrganization = ZendeskTestHelper.getJsonFromFile("data/simple_organization.json");
        JsonNode dataJsonUser = ZendeskTestHelper.getJsonFromFile("data/simple_user.json");
        // contain 2 records but 1 later records
        JsonNode dataJsonUserEvent = ZendeskTestHelper.getJsonFromFile("data/user_event_multiple.json");
        when(zendeskRestClient.doGet(any(), any(), anyBoolean()))
                .thenReturn(dataJsonOrganization.toString())
                .thenReturn(dataJsonUser.toString())
                .thenReturn(dataJsonUserEvent.toString());

        TaskReport taskReport = zendeskUserEventService.execute(0, recordImporter);
        // Next start time will be the latter imported records time
        assertEquals(expectedNextStartTime, taskReport.get(JsonNode.class, ZendeskConstants.Field.START_TIME).asLong());
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
