package org.embulk.input.zendesk.utils;

import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

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
        validateCredentials(task);
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

    private static void validateCredentials(final ZendeskInputPlugin.PluginTask task)
    {
        switch (task.getAuthenticationMethod()) {
            case OAUTH:
                if (!task.getAccessToken().isPresent()) {
                    throw new ConfigException(String.format("access_token is required for authentication method '%s'",
                            task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            case TOKEN:
                if (!task.getUsername().isPresent() || !task.getToken().isPresent()) {
                    throw new ConfigException(String.format("username and token are required for authentication method '%s'",
                            task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            case BASIC:
                if (!task.getUsername().isPresent() || !task.getPassword().isPresent()) {
                    throw new ConfigException(String.format("username and password are required for authentication method '%s'",
                            task.getAuthenticationMethod().name().toLowerCase()));
                }
                break;
            default:
                throw new ConfigException("Unknown authentication method");
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
        if (task.getIncremental()) {
            if (!task.getDedup()) {
                logger.warn("You've selected to skip de-duplicating records, result may contain duplicated data");
            }

            if (!ZendeskUtils.isSupportAPIIncremental(task.getTarget()) && task.getStartTime().isPresent()) {
                logger.warn(String.format("Target: '%s' doesn't support incremental export API. Will be ignored start_time option",
                        task.getTarget()));
            }
        }
    }
}
