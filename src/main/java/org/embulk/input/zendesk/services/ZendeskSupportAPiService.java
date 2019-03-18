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
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;

import java.util.stream.Collectors;

public class ZendeskSupportAPIService
{
    private ZendeskRestClient zendeskRestClient;

    private final PluginTask task;

    public ZendeskSupportAPIService(final PluginTask task)
    {
        this.task = task;
    }

    public JsonNode getData(String path, final int page, final boolean isPreview)
    {
        if (path.isEmpty()) {
            path = buildPath(page, isPreview);
        }

        try {
            final String response = getZendeskRestClient().doGet(path,
                    new StringJetty92ResponseEntityReader(ZendeskConstants.Misc.READ_TIMEOUT_IN_MILLIS_FOR_PREVIEW));
            final StringJsonParser jsonParser = new StringJsonParser();
            return jsonParser.parseJsonObject(response);
        }
        catch (final HttpResponseException ex) {
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
        final boolean isIncludeRelatedObjects = !task.getIncludes().isEmpty() && ZendeskUtils.isSupportInclude(task.getTarget());
        if (isIncludeRelatedObjects) {
            final boolean isRunWithTicketMetric = !isPreview && Target.TICKET_METRICS.equals(task.getTarget());
            if (isRunWithTicketMetric) {
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
        final boolean isIncrementalNeeded = Target.TICKET_EVENTS.equals(task.getTarget());

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
        if (Target.TICKET_METRICS.equals(task.getTarget())) {
            return buildURLForRunBasedOnTarget(Target.TICKETS, page)
                    .append("&include=metric_sets")
                    .toString();
        }
        return buildURLForRunBasedOnTarget(task.getTarget(), page).toString();
    }

    private StringBuilder buildURLForRunBasedOnTarget(final Target target, final int page)
    {
        final String startTime = task.getStartTime().map(s -> String.valueOf(ZendeskDateUtils.isoToEpochSecond(s))).orElse("0");

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

    private ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            int retryLimit = Exec.isPreview() ? 1 : task.getRetryLimit();
            final Jetty92RetryHelper retryHelper = new Jetty92RetryHelper(retryLimit, task.getRetryInitialWaitSec() * 1000,
                    task.getMaxRetryWaitSec() * 1000,
                    new DefaultJetty92ClientCreator(task.getConnectionTimeout() * 1000, task.getConnectionTimeout() * 1000));
            zendeskRestClient = new ZendeskRestClient(retryHelper, task);
        }
        return zendeskRestClient;
    }
}
