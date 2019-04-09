package org.embulk.input.zendesk.clients;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.Header;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.utils.ZendeskPluginTestRuntime;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;

import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

public class TestZendeskRestClient
{
    private final ExpectedException thrown = ExpectedException.none();
    private final TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "zendesk", ZendeskInputPlugin.class)
            .build();
    public ZendeskPluginTestRuntime runtime = new ZendeskPluginTestRuntime();

    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public TestRule chain = RuleChain.outerRule(embulk).around(runtime).around(thrown);

    private ZendeskRestClient zendeskRestClient;
    private PluginTask task = ZendeskTestHelper.getConfigSource("incremental.yml").loadConfig(PluginTask.class);
    private JsonNode data = ZendeskTestHelper.getJsonFromFile("data/client.json");

    private HttpClient client = Mockito.mock(HttpClient.class);
    private HttpResponse response = Mockito.mock(HttpResponse.class);
    private Header header =  mock(Header.class);
    private StatusLine statusLine = Mockito.mock(StatusLine.class);

    @Before
    public void prepare() throws IOException
    {
        zendeskRestClient = spy(new ZendeskRestClient());

        when(zendeskRestClient.createHttpClient()).thenReturn(client);
        when(client.execute(any())).thenReturn(response);
        when(response.getStatusLine()).thenReturn(statusLine);
    }

    @Test
    public void doGetSuccess()
    {
        setup("doGet200");
        JsonNode expectedResult = ZendeskTestHelper.getJsonFromFile("data/tickets.json");
        String result = zendeskRestClient.doGet("dummyString", task, false);
        assertEquals(expectedResult.toString(), result);
    }

    @Test
    public void doGetRetryFail429WithoutRetryAfter()
    {
        String expectedMessage = "Status: '429', error message: 'Number of allowed incremental export API requests per minute exceeded'";
        int expectedRetryTime = 3;
        testExceptionMessageForDoGet("doGet429", expectedMessage, expectedRetryTime);
    }

    @Test
    public void doGetRetryFail429WithRetryAfter()
    {
        String expectedMessage = "Status: '429', error message: 'Number of allowed incremental export API requests per minute exceeded'";
        int expectedRetryTime = 3;

        when(response.containsHeader("x-rate-limit")).thenReturn(true);
        when(response.getFirstHeader("x-rate-limit")).thenReturn(header);
        when(response.getFirstHeader("Retry-After")).thenReturn(header);
        when(header.getValue())
                .thenReturn("5");

        setupData("doGet429");
        verifyData(expectedMessage, expectedRetryTime);
    }

    @Test
    public void doGetRetry405()
    {
        String expectedMessage = "Status '405', message 'dummy text'";
        int expectedRetryTime = 1;

        testExceptionMessageForDoGet("doGet405", expectedMessage, expectedRetryTime);
    }

    @Test
    public void doGetRetry422FailBecauseNotContainTooRecentStartTime()
    {
        String expectedMessage = "Status: '422', error message: 'dummy text'";
        int expectedRetryTime = 1;
        testExceptionMessageForDoGet("doGet422NotContainTooRecentStartTime", expectedMessage, expectedRetryTime);
    }

    @Test
    public void doGetNotRetry422BecauseContainTooRecentStartTime()
    {
        String expectedMessage = "Status: '422', error message: 'dummy text'";
        int expectedRetryTime = 1;
        testExceptionMessageForDoGet("doGet422ContainTooRecentStartTime", expectedMessage, expectedRetryTime);
    }

    @Test
    public void doGetRetrySuccess429()
    {
        setupReTrySuccess("doGet429");
    }

    @Test
    public void doGetRetry500()
    {
        setupReTrySuccess("doGet500");
    }

    @Test
    public void doGetRetry503()
    {
        setupReTrySuccess("doGet503");
    }

    @Test
    public void doGetRetry404()
    {
        setup("doGet404");
        String expectedMessage = "Status: '404', error message: 'dummy text'";
        int expectedRetryTime = 1;
        try {
            zendeskRestClient.doGet("any", task, false);
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(expectedMessage, e.getMessage());
        }
        verify(zendeskRestClient, times(expectedRetryTime)).createHttpClient();
    }

    @Test
    public void doGetRetry409()
    {
        setupReTrySuccess("doGet409");
    }

    @Test
    public void doGetRetryWhenStatusNot400s()
    {
        setupReTrySuccess("doGet502");
    }

    @Test
    public void doGetRetry503WithRetryAfterNegative()
    {
        setup("doGet503");

        int expectedRetryTime = 3;
        when(response.getFirstHeader("Retry-After")).thenReturn(header);
        when(header.getValue())
                .thenReturn("5")
                .thenReturn("-5");

        try {
            zendeskRestClient.doGet("any", task, false);
        }
        catch (final Exception e) {
        }
        verify(zendeskRestClient, times(expectedRetryTime)).createHttpClient();
    }

    @Test
    public void doGetRetryWhenThrowIOException() throws IOException
    {
        setupRateLimit();

        when(client.execute(any()))
                .thenThrow(new IOException());

        int expectedRetryTime = 3;
        assertThrows(ConfigException.class, () -> zendeskRestClient.doGet("any", task, false));
        verify(zendeskRestClient, times(expectedRetryTime)).createHttpClient();
    }

    @Test
    public void authenticationOauthSuccess() throws IOException
    {
        setup("doGet200");

        String accessToken = "testzendesk";

        ConfigSource configSource = ZendeskTestHelper.getConfigSource("incremental.yml");
        configSource.set("auth_method", "oauth");
        configSource.set("access_token", accessToken);
        PluginTask pluginTask = configSource.loadConfig(PluginTask.class);

        String expectedValue = "Bearer " + accessToken;
        setupAndVerifyAuthenticationString(expectedValue, pluginTask);
    }

    @Test
    public void authenticationBasicSuccess() throws IOException
    {
        setup("doGet200");

        String username = "zendesk_username";
        String password = "zendesk_password";

        ConfigSource configSource = ZendeskTestHelper.getConfigSource("incremental.yml");
        configSource.set("auth_method", "basic");
        configSource.set("username", Optional.of(username));
        configSource.set("password", password);
        PluginTask pluginTask = configSource.loadConfig(PluginTask.class);

        String expectedValue = "Basic " + ZendeskUtils.convertBase64(String.format("%s:%s", username, password));
        setupAndVerifyAuthenticationString(expectedValue, pluginTask);
    }

    @Test
    public void authenticationTokenSuccess() throws IOException
    {
        setup("doGet200");

        String username = "zendesk_username";
        String token = "zendesk_token";

        ConfigSource configSource = ZendeskTestHelper.getConfigSource("incremental.yml");
        configSource.set("auth_method", "token");
        configSource.set("username", username);
        configSource.set("token", token);
        PluginTask pluginTask = configSource.loadConfig(PluginTask.class);

        String expectedValue = "Basic " + ZendeskUtils.convertBase64(String.format("%s/token:%s", username, token));
        setupAndVerifyAuthenticationString(expectedValue, pluginTask);
    }

    private void setupRateLimit()
    {
        when(response.containsHeader("x-rate-limit")).thenReturn(true);
        when(response.getFirstHeader("x-rate-limit")).thenReturn(header);
        when(header.getValue()).thenReturn("400");
    }

    private void setup(String name)
    {
        setupRateLimit();
        setupData(name);
    }

    private void setupData(String name)
    {
        JsonNode messageResponse = data.get(name);
        when(statusLine.getStatusCode()).thenReturn(messageResponse.get("statusCode").asInt());
        try {
            if (name.equals("doGet200")) {
                doReturn(new StringEntity(messageResponse.get("body").toString())).when(response).getEntity();
            }
            else {
                doReturn(new StringEntity(messageResponse.get("description").asText())).when(response).getEntity();
            }
        }
        catch (Exception e) {
            fail("fail to setup client for UT");
        }
    }

    private void testExceptionMessageForDoGet(String name, String expectedMessage, int expectedRetryTime)
    {
        setup(name);
        verifyData(expectedMessage, expectedRetryTime);
    }

    private void verifyData(String expectedMessage, int expectedRetryTime)
    {
        try {
            zendeskRestClient.doGet("any", task, false);
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(expectedMessage, e.getMessage());
        }
        verify(zendeskRestClient, times(expectedRetryTime)).createHttpClient();
    }

    private void setupReTrySuccess(String entityName)
    {
        setupRateLimit();

        JsonNode messageResponse = data.get(entityName);
        JsonNode messageResponseSuccess = data.get("doGet200");

        when(statusLine.getStatusCode())
                .thenReturn(messageResponse.get("statusCode").asInt())
                .thenReturn(messageResponseSuccess.get("statusCode").asInt());

        try {
            when(response.getEntity())
                    .thenReturn(new StringEntity(messageResponse.get("description").asText()))
                    .thenReturn(new StringEntity(messageResponseSuccess.get("body").toString()));
        }
        catch (Exception e) {
            fail("fail to setup client for UT");
        }

        int expectedRetryTime = 2;
        zendeskRestClient.doGet("any", task, false);
        verify(zendeskRestClient, times(expectedRetryTime)).createHttpClient();
    }

    private void setupAndVerifyAuthenticationString(String expectedString, PluginTask pluginTask) throws IOException
    {
        zendeskRestClient.doGet("any", pluginTask, false);
        final ArgumentCaptor<HttpRequestBase> request = ArgumentCaptor.forClass(HttpRequestBase.class);
        verify(client).execute(request.capture());

        String credential = request.getValue().getFirstHeader(AUTHORIZATION).getValue();
        assertEquals(expectedString, credential);
    }
}
