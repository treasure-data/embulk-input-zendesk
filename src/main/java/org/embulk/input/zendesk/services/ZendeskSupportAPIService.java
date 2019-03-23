package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;

import java.io.IOException;
import java.util.stream.Collectors;

public class ZendeskSupportAPIService
{
    private ZendeskRestClient zendeskRestClient;

    private PluginTask task;

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    public ZendeskSupportAPIService(final PluginTask task)
    {
        this.task = task;
    }

    public void setTask(PluginTask task)
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

            return parseJsonObject(response);
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

    private ObjectNode parseJsonObject(String jsonText)
    {
        JsonNode node = parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        }
        else {
            throw new DataException("Expected object node: " + jsonText);
        }
    }

    private JsonNode parseJsonNode(String jsonText)
    {
        try {
            return mapper.readTree(jsonText);
        }
        catch (IOException e) {
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
        final boolean isSupportIncremental = ZendeskUtils.isSupportIncremental(task.getTarget());
        StringBuilder previewURL = new StringBuilder(task.getLoginUrl());

        previewURL.append(isSupportIncremental
                        ? ZendeskConstants.Url.API_INCREMENTAL
                        : ZendeskConstants.Url.API)
                .append("/");

        if (Target.TICKET_METRICS.equals(task.getTarget())) {
            previewURL.append(Target.TICKETS.toString())
                    .append(".json?")
                    .append("include=metric_sets");
        }
        else {
            previewURL.append(task.getTarget().toString())
                    .append(".json?");
        }

        previewURL.append(isSupportIncremental
            ? "&start_time=0"
            : "&per_page=1");

        return previewURL.toString();
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
        String startTime = "";
        boolean isSupportIncremental = ZendeskUtils.isSupportIncremental(target);
        if (isSupportIncremental) {
            startTime = task.getStartTime().map(s -> String.valueOf(ZendeskDateUtils.isoToEpochSecond(s))).orElse("0");
        }
        return new StringBuilder(task.getLoginUrl())
                .append(isSupportIncremental
                        ? ZendeskConstants.Url.API_INCREMENTAL
                        : ZendeskConstants.Url.API)
                .append("/")
                .append(target.toString())
                .append(isSupportIncremental
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
