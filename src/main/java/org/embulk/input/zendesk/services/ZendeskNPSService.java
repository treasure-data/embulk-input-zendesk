package org.embulk.input.zendesk.services;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;

public class ZendeskNPSService extends ZendeskNormalServices
{
    public ZendeskNPSService(final ZendeskInputPlugin.PluginTask task)
    {
        super(task);
    }

    public boolean isSupportIncremental()
    {
        return true;
    }
    @Override
    protected String buildURI(final int page, final long startTime, final long endTime)
    {
        return buildURI(page, startTime);
    }

    @Override
    protected String buildURI(final int page, final long startTime)
    {
        return ZendeskUtils.getURIBuilder(task.getLoginUrl())
                .setPath(ZendeskConstants.Url.API_NPS_INCREMENTAL
                        + "/"
                        + task.getTarget().getJsonName()
                        + ".json")
                .setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime))
                .toString();
    }
}
