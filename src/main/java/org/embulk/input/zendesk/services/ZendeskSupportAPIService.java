package org.embulk.input.zendesk.services;

import org.apache.http.client.utils.URIBuilder;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;

public class ZendeskSupportAPIService extends ZendeskNormalServices
{
    public ZendeskSupportAPIService(final PluginTask task)
    {
        super(task);
    }

    public boolean isSupportIncremental()
    {
        return !(task.getTarget().equals(Target.TICKET_FORMS)
                || task.getTarget().equals(Target.TICKET_FIELDS)
                || task.getTarget().equals(Target.SATISFACTION_RATINGS));
    }

    @Override
    protected String buildURI(final int page, long startTime)
    {
        final URIBuilder uriBuilder = ZendeskUtils.getURIBuilder(task.getLoginUrl()).setPath(buildPath());

        if (isSupportIncremental()) {
            uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime));
            if (Target.TICKET_METRICS.equals(task.getTarget())) {
                uriBuilder.setParameter("include", "metric_sets");
            }
        }
        else {
            if (Target.SATISFACTION_RATINGS.equals(task.getTarget())){
                uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime))
                        .setParameter("sort_by", "id")
                        .setParameter("per_page", String.valueOf(100))
                        .setParameter("page", String.valueOf(page));

            }
            else{
                uriBuilder.setParameter("sort_by", "id")
                        .setParameter("per_page", String.valueOf(100))
                        .setParameter("page", String.valueOf(page));
            }
        }

        return uriBuilder.toString();
    }

    private String buildPath()
    {
        return (isSupportIncremental() && !(Target.SATISFACTION_RATINGS.equals(task.getTarget()))
                ? ZendeskConstants.Url.API_INCREMENTAL
                : ZendeskConstants.Url.API) +
                "/" +
                (Target.TICKET_METRICS.equals(task.getTarget())
                        ? Target.TICKETS.toString()
                        : task.getTarget().toString())
                + ".json";
    }
}
