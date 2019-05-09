package org.embulk.input.zendesk.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.config.ConfigSource;

import org.embulk.input.zendesk.ZendeskInputPlugin;

import org.embulk.input.zendesk.models.Target;
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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    @Before
    public void prepare()
    {
        configSource = getBaseConfigSource();
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
        ZendeskValidatorUtils.validateInputTask(task);
    }

    @Test
    public void validateIncremental()
    {
        configSource.set("target", "ticket_fields");
        configSource.set("incremental", true);
        configSource.set("dedup", false);
        configSource.set(ZendeskConstants.Field.START_TIME, "");

        final ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task);
    }

    @Test
    public void validateCustomObjectShouldThrowException()
    {
        configSource.set("target", Target.OBJECT_RECORDS.name().toLowerCase());
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "Should have at least one Object Type");

        configSource.set("target", Target.RELATIONSHIP_RECORDS.name().toLowerCase());
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "Should have at least one Relationship Type");
    }

    @Test
    public void validateCustomObject()
    {
        configSource.set("target", Target.OBJECT_RECORDS.name().toLowerCase());
        List<String> objectTypes = new ArrayList<>();
        objectTypes.add("user");
        objectTypes.add("account");
        configSource.set("object_types", objectTypes);
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task);

        configSource.set("target", Target.RELATIONSHIP_RECORDS.name().toLowerCase());
        configSource.set("relationship_types", Collections.singletonList("dummy"));
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task);
    }

    @Test
    public void validateUserEventShouldThrowException()
    {
        configSource.set("target", Target.USER_EVENTS.name().toLowerCase());
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "Profile Source is required for User Event Target");
    }

    @Test
    public void validateUserEvent()
    {
        configSource.set("target", Target.USER_EVENTS.name().toLowerCase());
        configSource.set("profile_source", "user");
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task);
    }

    @Test
    public void validateTimeShouldThrowException()
    {
        configSource.set("target", Target.TICKETS.name().toLowerCase());
        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));

        configSource.set("end_time", "2019-12-2 22-12-22");
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "End Time should follow these format " + ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT.toString());

        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() - 3600), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "End Time shouldn't be in the past");

        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() + 3600), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        assertValidation(task, "End Time should be later or equal than Start Time");
    }

    @Test
    public void validateTime()
    {
        configSource.set("target", Target.TICKETS.name().toLowerCase());
        configSource.set("start_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond() - 3600), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        configSource.set("end_time", OffsetDateTime.ofInstant(Instant.ofEpochSecond(Instant.now().getEpochSecond()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        ZendeskInputPlugin.PluginTask task = configSource.loadConfig(ZendeskInputPlugin.PluginTask.class);
        ZendeskValidatorUtils.validateInputTask(task);
    }

    private static ConfigSource getBaseConfigSource()
    {
        return ZendeskTestHelper.getConfigSource("base_validator.yml");
    }

    private void assertValidation(final ZendeskInputPlugin.PluginTask task, final String message)
    {
        try {
            ZendeskValidatorUtils.validateInputTask(task);
            fail("Should not reach here");
        }
        catch (final Exception e) {
            assertEquals(message, e.getMessage());
        }
    }
}
