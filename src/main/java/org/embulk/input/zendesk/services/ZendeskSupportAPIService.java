package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;

public class ZendeskSupportAPIService extends ZendeskNormalServices
{
    public ZendeskSupportAPIService(final PluginTask task)
    {
        super(task);
    }

    public boolean isSupportIncremental()
    {
        return !(task.getTarget().equals(Target.TICKET_FORMS) || task.getTarget().equals(Target.TICKET_FIELDS));
    }

    @Override
    protected String buildURI(final int page, long startTime)
    {
        final URIBuilder uriBuilder = getURIBuilderFromHost().setPath(buildPath());

        if (isSupportIncremental()) {
            uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime));
            if (Target.TICKET_METRICS.equals(task.getTarget())) {
                uriBuilder.setParameter("include", "metric_sets");
            }
        }
        else {
            uriBuilder.setParameter("sort_by", "id")
                    .setParameter("per_page", String.valueOf(100))
                    .setParameter("page", String.valueOf(page));
        }

        return uriBuilder.toString();
    }

    private String buildPath()
    {
        return (isSupportIncremental()
                ? ZendeskConstants.Url.API_INCREMENTAL
                : ZendeskConstants.Url.API) +
                "/" +
                (Target.TICKET_METRICS.equals(task.getTarget())
                        ? Target.TICKETS.toString()
                        : task.getTarget().toString())
                + ".json";
    }
}
