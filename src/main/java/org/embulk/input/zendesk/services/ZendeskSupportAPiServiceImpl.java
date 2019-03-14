package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPluginDelegate.PluginTask;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;

import java.util.stream.Collectors;

public class ZendeskSupportAPiServiceImpl implements ZendeskSupportAPiService
{
    private final ZendeskRestClient zendeskRestClient;

    private final PluginTask task;

    public ZendeskSupportAPiServiceImpl(final ZendeskRestClient zendeskRestClient, final PluginTask task)
    {
        this.zendeskRestClient = zendeskRestClient;
        this.task = task;
    }

    @Override
    public JsonNode getData(String path, final int page, final boolean isPreview)
    {
        if (path.isEmpty()) {
            path = buildPath(page, isPreview);
        }

        try {
            final StringJsonParser jsonParser = new StringJsonParser();
            final String response = zendeskRestClient.doGet(path,
                    new StringJetty92ResponseEntityReader(ZendeskConstants.Misc.READ_TIMEOUT_IN_MILLIS_FOR_PREVIEW));
            return jsonParser.parseJsonObject(response);
        }
        catch (HttpResponseException ex) {
            if (ex.getResponse().getStatus() == 401) {
                throw new ConfigException("Invalid credential. Error 401: can't authenticate");
            }
            else if (ex.getResponse().getStatus() == 403) {
                throw new ConfigException("Forbidden access. Error 403: please check your permission again");
            }
            else {
                throw Throwables.propagate(ex);
            }
        }
        catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private String buildPath(final int page, final boolean isPreview)
    {
        final StringBuilder path = isPreview
                ? new StringBuilder(buildURLForPreview())
                : new StringBuilder(buildURLForRun(page));

        // Include some related objects
        if (!task.getIncludes().isEmpty() && ZendeskUtils.isSupportInclude(task.getTarget())) {
            if (!isPreview && task.getTarget() == Target.TICKET_METRICS) {
                path.append(",");
            }
            else {
                path.append("&include=");
            }
            path.append(task.getIncludes()
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.joining(",")));
        }

        return path.toString();
    }

    private String buildURLForPreview()
    {
        // For ticket_events we only have incremental end point
        return buildURLForPreviewBasedOnTarget(task.getTarget() == Target.TICKET_EVENTS);
    }

    private String buildURLForPreviewBasedOnTarget(boolean isIncrementalNeeded)
    {
        return new StringBuilder(task.getLoginUrl())
                .append(isIncrementalNeeded
                        ? ZendeskConstants.Url.API_INCREMENTAL
                        : ZendeskConstants.Url.API)
                .append("/")
                .append(task.getTarget().toString())
                .append(".json?")
                .append(isIncrementalNeeded
                        ? "start_time=0"
                        : "per_page=1").toString();
    }

    private String buildURLForRun(final int page)
    {
        if (task.getTarget() == Target.TICKET_METRICS) {
            return buildURLForRunBasedOnTarget(Target.TICKETS, page)
                    .append("&include=metric_sets")
                    .toString();
        }
        return buildURLForRunBasedOnTarget(task.getTarget(), page).toString();
    }

    private StringBuilder buildURLForRunBasedOnTarget(final Target target, final int page)
    {
        String startTime = task.getStartTime().isPresent()
                            ? String.valueOf(ZendeskDateUtils.toTimeStamp(task.getStartTime().get()))
                            : "0";

        return new StringBuilder(task.getLoginUrl())
                .append(task.getIncremental()
                        ? ZendeskConstants.Url.API_INCREMENTAL
                        : ZendeskConstants.Url.API)
                .append("/")
                .append(target.toString())
                .append(task.getIncremental()
                        ? ".json?start_time=" + startTime
                        : ".json?sort_by=id&per_page=100&page=" + page);
    }
}
