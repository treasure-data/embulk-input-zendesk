package org.embulk.input.zendesk.utils;

import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZendeskValidatorUtils
{
    private ZendeskValidatorUtils(){}

    private static final Logger logger = Exec.getLogger(ZendeskValidatorUtils.class);

    public static void validateInputTask(final ZendeskInputPlugin.PluginTask task, final ZendeskSupportAPIService zendeskSupportAPIService)
    {
        validateHost(task.getLoginUrl());
        validateAppMarketPlace(task.getAppMarketPlaceIntegrationName().isPresent(),
                task.getAppMarketPlaceAppId().isPresent(),
                task.getAppMarketPlaceOrgId().isPresent());
        validateCredentials(task, zendeskSupportAPIService);
        validateInclude(task.getIncludes(), task.getTarget());
        validateIncremental(task);
    }

    private static void validateHost(final String loginUrl)
    {
        final Matcher matcher = Pattern.compile(ZendeskConstants.Regex.HOST).matcher(loginUrl);
        if (!matcher.matches()) {
            throw new ConfigException(String.format("Login URL, '%s', is unmatched expectation. " +
                    "It should be followed this format: https://abc.zendesk.com/", loginUrl));
        }
    }

    private static void validateInclude(final List<String> includes, final Target target)
    {
        if (includes != null && !includes.isEmpty()) {
            if (!ZendeskUtils.isSupportInclude(target)) {
                logger.warn("Target: '{}' doesn't support include size loading. Option include will be ignored", target.toString());
            }
        }
    }

    private static void validateCredentials(final ZendeskInputPlugin.PluginTask task, final ZendeskSupportAPIService zendeskSupportAPIService)
    {
        switch (task.getAuthenticationMethod()) {
            case OAUTH:
                if (!task.getAccessToken().isPresent()) {
                    throw new ConfigException(String.format("access_token is required for authentication method '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case TOKEN:
                if (!task.getUsername().isPresent() || !task.getToken().isPresent()) {
                    throw new ConfigException(String.format("username and token are required for authentication method '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            case BASIC:
                if (!task.getUsername().isPresent() || !task.getPassword().isPresent()) {
                    throw new ConfigException(String.format("username and password are required for authentication method '%s'",
                            task.getAuthenticationMethod().name()));
                }
                break;
            default:
                throw new ConfigException("Unknown authentication method");
        }

        // Validate credentials by sending one request to users.json. It Should always have at least one user
        try {
            zendeskSupportAPIService.getData(String.format("%s%s/users.json?per_page=1", task.getLoginUrl(),
                    ZendeskConstants.Url.API), 0, false);
        }
        catch (final HttpResponseException ex) {
            if (ex.getResponse().getStatus() == 401) {
                throw new ConfigException("Invalid credential. Error 401: can't authenticate");
            }
        }
    }

    private static void validateAppMarketPlace(final boolean isAppMarketIntegrationNamePresent,
                                        final boolean isAppMarketAppIdPresent,
                                        final boolean isAppMarketOrgIdPresent)
    {
        final boolean isAllAvailable =
                isAppMarketIntegrationNamePresent && isAppMarketAppIdPresent && isAppMarketOrgIdPresent;
        final boolean isAllUnAvailable =
                !isAppMarketIntegrationNamePresent && !isAppMarketAppIdPresent && !isAppMarketOrgIdPresent;
        // All or nothing needed
        if (!(isAllAvailable || isAllUnAvailable)) {
            throw new ConfigException("All of app_marketplace_integration_name, app_marketplace_org_id, " +
                    "app_marketplace_app_id " +
                    "are required to fill out for Apps Marketplace API header");
        }
    }

    private static void validateIncremental(final ZendeskInputPlugin.PluginTask task)
    {
        // auto run with non incremental if target doesn't support incremental
        if (!ZendeskUtils.isSupportIncremental(task.getTarget())) {
            task.setIncremental(false);
        }

        if (task.getIncremental()) {
            if (!task.getDedup()) {
                logger.warn("You've selected to skip de-duplicating records, result may contain duplicated data");
            }
        }
        else {
            if (task.getStartTime().isPresent()) {
                task.setStartTime(Optional.of(ZendeskConstants.Misc.DEFAULT_START_TIME));
            }

            if (ZendeskUtils.isSupportIncremental(task.getTarget())) {
                task.setIncremental(true);
            }
        }

        if (task.getIncremental()) {
            if (!ZendeskDateUtils.isSupportedTimeFormat(task.getStartTime().get(),
                    ImmutableList.of(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT,
                            ZendeskConstants.Misc.SHORT_DATE_FORMAT))) {
                // it followed the logic in the old version
                task.setStartTime(Optional.of(ZendeskConstants.Misc.DEFAULT_START_TIME));
            }
        }
    }
}
