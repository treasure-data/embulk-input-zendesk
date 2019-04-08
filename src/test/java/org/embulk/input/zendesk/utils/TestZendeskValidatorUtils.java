package org.embulk.input.zendesk.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigSource;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;

import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.mock;

import java.util.Collections;

public class TestZendeskValidatorUtils
{
    private final ExpectedException thrown = ExpectedException.none();
    private final TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "zendesk", ZendeskInputPlugin.class)
            .build();
    public ZendeskPluginTestRuntime runtime = new ZendeskPluginTestRuntime();
    ConfigSource configSource;

    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public TestRule chain = RuleChain.outerRule(embulk).around(runtime).around(thrown);

    private ZendeskSupportAPIService zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);

    @Before
    public void prepare()
    {
        configSource = getBaseConfigSource();
    }

    @Test
    public void validateHostShouldThrowException()
    {
        configSource.set("login_url", "dummyhost");
        final ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "Login URL, 'dummyhost', is unmatched expectation. It should be followed this format: https://abc.zendesk.com/");
    }

    @Test
    public void validateCredentialOauthShouldThrowException()
    {
        configSource.set("auth_method", "oauth");
        configSource.remove("access_token");
        final ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "access_token is required for authentication method 'oauth'");
    }

    @Test
    public void validateCredentialBasicShouldThrowException()
    {
        configSource.set("auth_method", "basic");
        configSource.remove("username");
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "username and password are required for authentication method 'basic'");

        configSource.set("username", "");
        configSource.remove("password");
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "username and password are required for authentication method 'basic'");
    }

    @Test
    public void validateCredentialTokenShouldThrowException()
    {
        configSource.set("auth_method", "token");
        configSource.remove("token");
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "username and token are required for authentication method 'token'");

        configSource.set("token", "");
        configSource.remove("username");
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "username and token are required for authentication method 'token'");
    }

    @Test
    public void validateAppMarketPlaceShouldThrowException()
    {
        configSource.remove("app_marketplace_integration_name");
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "All of app_marketplace_integration_name, app_marketplace_org_id, " +
                "app_marketplace_app_id " +
                "are required to fill out for Apps Marketplace API header");
    }

    @Test
    public void validateInclude()
    {
        configSource.set("target", "ticket_fields");
        configSource.set("includes", Collections.singletonList("organizations"));
        final ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task, zendeskSupportAPIService);
    }

    @Test
    public void validateIncremental()
    {
        configSource.set("target", "ticket_fields");
        configSource.set("incremental", true);
        configSource.set("dedup", false);
        configSource.set("start_time", "");

        final ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task, zendeskSupportAPIService);
    }

    private static ConfigSource getBaseConfigSource()
    {
        return ZendeskTestHelper.getConfigSource("base_validator.yml");
    }

    private void assertValidation(final ZendeskInputPlugin.PluginTask task, final String message)
    {
        try {
            ZendeskValidatorUtils.validateInputTask(task, zendeskSupportAPIService);
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(message, e.getMessage());
        }
    }
}
