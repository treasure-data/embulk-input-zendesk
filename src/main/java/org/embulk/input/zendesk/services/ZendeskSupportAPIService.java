package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;

import java.io.IOException;

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

    public JsonNode getData(String path, final int page, final boolean isPreview, long startTime)
    {
        if (path.isEmpty()) {
            path = buildPath(page, startTime);
        }
        try {
            final String response = getZendeskRestClient().doGet(path, task, isPreview);
            return parseJsonObject(response);
        }
        catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            zendeskRestClient = new ZendeskRestClient();
        }
        return zendeskRestClient;
    }

    private ObjectNode parseJsonObject(final String jsonText)
    {
        JsonNode node = parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        }

        throw new DataException("Expected object node to parse but doesn't get");
    }

    private JsonNode parseJsonNode(final String jsonText)
    {
        try {
            return mapper.readTree(jsonText);
        }
        catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private String buildPath(final int page, long startTime)
    {
        boolean isSupportIncremental = ZendeskUtils.isSupportAPIIncremental(task.getTarget());

        StringBuilder urlBuilder = new StringBuilder(task.getLoginUrl())
                .append(isSupportIncremental
                        ? ZendeskConstants.Url.API_INCREMENTAL
                        : ZendeskConstants.Url.API)
                .append("/")
                .append(Target.TICKET_METRICS.equals(task.getTarget())
                        ? Target.TICKETS.toString()
                        : task.getTarget().toString())
                .append(".json?");

        urlBuilder
                .append(isSupportIncremental
                        ? "start_time=" + startTime
                        : "sort_by=id&per_page=100&page=" + page);

        if (Target.TICKET_METRICS.equals(task.getTarget())) {
            urlBuilder.append("&include=metric_sets");
        }

        return urlBuilder.toString();
    }
}
