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
                || task.getTarget().equals(Target.SLA_POLICIES)
                || task.getTarget().equals(Target.TICKET_FIELDS)
                || task.getTarget().equals(Target.SATISFACTION_RATINGS)
                || task.getTarget().equals(Target.GROUPS));
    }

    @Override
    protected String buildURI(final int page, long startTime){
        return buildURI(page, startTime);
    }

    @Override
    protected String buildURI(final int page, long startTime, long endTime)
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
                if (endTime>0){
                    uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime))
                            .setParameter(ZendeskConstants.Field.END_TIME, String.valueOf(endTime))
                            .setParameter("sort_by", "id")
                            .setParameter("per_page", String.valueOf(100))
                            .setParameter("page", String.valueOf(page));
                }else{
                    uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime))
                            .setParameter("sort_by", "id")
                            .setParameter("per_page", String.valueOf(100))
                            .setParameter("page", String.valueOf(page));
                }
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
        if(Target.SLA_POLICIES.equals(task.getTarget()))
        {
            return ZendeskConstants.Url.API+"/slas/policies";
        }
        else{
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
}
