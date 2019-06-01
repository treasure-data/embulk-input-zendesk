package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.http.HttpStatus;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.stream.paginator.sunshine.CustomObjectSpliterator;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ZendeskCustomObjectService implements ZendeskService
{
    protected ZendeskInputPlugin.PluginTask task;

    private ZendeskRestClient zendeskRestClient;

    public ZendeskCustomObjectService(final ZendeskInputPlugin.PluginTask task)
    {
        this.task = task;
    }

    public boolean isSupportIncremental()
    {
        return false;
    }

    @Override
    public TaskReport addRecordToImporter(final int taskIndex, final RecordImporter recordImporter)
    {
        final List<String> paths = getListPathByTarget();

        paths.parallelStream().forEach(path -> StreamSupport.stream(new CustomObjectSpliterator(path, getZendeskRestClient(), task, Exec.isPreview()), Exec.isPreview())
                .forEach(recordImporter::addRecord));

        return Exec.newTaskReport();
    }

    @Override
    public JsonNode getDataFromPath(final String path, final int page, final boolean isPreview, final long startTime)
    {
        Preconditions.checkArgument(isPreview, "IsPreview should be true to use this method");

        Optional<String> response = Optional.empty();
        final List<String> paths = path.isEmpty()
                ? getListPathByTarget()
                : Collections.singletonList(path);

        for (final String temp : paths) {
            try {
                response = Optional.ofNullable(getZendeskRestClient().doGet(temp, task, true));

                if (response.isPresent()) {
                    break;
                }
            }
            catch (final ConfigException e) {
                // Sometimes we get 404 when having invalid endpoint, so ignore when we get 404
                if (!(e.getCause() instanceof ZendeskException && ((ZendeskException) e.getCause()).getStatusCode() == HttpStatus.SC_NOT_FOUND)) {
                    throw e;
                }
            }
        }

        return response.map(ZendeskUtils::parseJsonObject).orElse(new ObjectMapper().createObjectNode());
    }

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            zendeskRestClient = new ZendeskRestClient();
        }
        return zendeskRestClient;
    }

    private List<String> getListPathByTarget()
    {
        return task.getTarget().equals(Target.OBJECT_RECORDS)
                ? task.getObjectTypes().stream().map(this::buildPath).collect(Collectors.toList())
                : task.getRelationshipTypes().stream().map(this::buildPath).collect(Collectors.toList());
    }

    private String buildPath(final String value)
    {
        final String perPage = Exec.isPreview() ? "1" : "1000";

        return ZendeskUtils.getURIBuilder(task.getLoginUrl())
                .setPath(task.getTarget().equals(Target.OBJECT_RECORDS)
                        ? ZendeskConstants.Url.API_OBJECT_RECORD
                        : ZendeskConstants.Url.API_RELATIONSHIP_RECORD)
                .setParameter("type", value)
                .setParameter("per_page", perPage)
                .toString();
    }
}
