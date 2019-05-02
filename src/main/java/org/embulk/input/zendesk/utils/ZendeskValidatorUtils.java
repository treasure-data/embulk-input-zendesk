package org.embulk.input.zendesk.utils;

import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.Target;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.time.Instant;

public class ZendeskValidatorUtils
{
    private ZendeskValidatorUtils(){}

    private static final Logger logger = Exec.getLogger(ZendeskValidatorUtils.class);

    private static ZendeskInputPlugin.PluginTask task;

    public static void validateInputTask(final ZendeskInputPlugin.PluginTask task)
    {
        ZendeskValidatorUtils.task = task;
        validateAppMarketPlace(task.getAppMarketPlaceIntegrationName().isPresent(),
                task.getAppMarketPlaceAppId().isPresent(),
                task.getAppMarketPlaceOrgId().isPresent());
        validateCredentials();
        validateIncremental();
        validateCustomObject();
        validateUserEvent();
        validateTime();
    }

    private static void validateCredentials()
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

    private static void validateIncremental()
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

    private static void validateCustomObject()
    {
        if (task.getTarget().equals(Target.OBJECT_RECORDS) && task.getObjectTypes().isEmpty()) {
                throw new ConfigException("Should have at least one Object Type");
        }

        if (task.getTarget().equals(Target.RELATIONSHIP_RECORDS) && task.getRelationshipTypes().isEmpty()) {
            throw new ConfigException("Should have at least one Relationship Type");
        }
    }

    private static void validateUserEvent()
    {
        if (task.getTarget().equals(Target.USER_EVENTS) && !task.getProfileSource().isPresent()) {
            throw new ConfigException("Profile Source is required for User Event Target");
        }
    }

    private static void validateTime()
    {
        // if we can't parse start_time, it will automatically set to 0
        task.getStartTime().ifPresent(time -> {
            if (ZendeskDateUtils.supportedTimeFormat(task.getStartTime().get()).isPresent()
                    && ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get()) > Instant.now().getEpochSecond()) {
                    throw new ConfigException("Start Time shouldn't be in the future");
                }
            });

        // Can't set end_time to 0, so it should be valid
        task.getEndTime().ifPresent(time -> {
            if (!ZendeskDateUtils.supportedTimeFormat(task.getEndTime().get()).isPresent()) {
                throw new ConfigException("End Time should follow these format " + ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT.toString());
            }

            if (ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get()) > Instant.now().getEpochSecond()) {
                throw new ConfigException("End Time shouldn't be in the future");
            }
        });

        if (task.getStartTime().isPresent() && task.getEndTime().isPresent()
                && ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get()) > ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get())) {
            throw new ConfigException("End Time should be later or equal than Start Time");
        }
    }
}
