package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.stream.paginator.CustomObjectSpliterator;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ZendeskCustomObjectService extends ZendeskBaseServices implements ZendeskService
{
    public ZendeskCustomObjectService(final ZendeskInputPlugin.PluginTask task)
    {
        super(task);
    }

    @Override
    protected String buildURI(final int page, final long startTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskReport execute(final int taskIndex, final Schema schema, final PageBuilder pageBuilder)
    {
        final List<String> paths = getListPathByTarget();

        paths.parallelStream().forEach(path -> StreamSupport.stream(new CustomObjectSpliterator(path, getZendeskRestClient(), task, Exec.isPreview()), Exec.isPreview())
                .forEach(jsonNode -> addRecord(jsonNode, schema, pageBuilder)));

        return Exec.newTaskReport();
    }

    @Override
    public JsonNode getData(final String path, final int page, final boolean isPreview, final long startTime)
    {
        Optional<String> response = Optional.empty();

        final List<String> paths = path.isEmpty()
                ? getListPathByTarget()
                : Collections.singletonList(path);

        for (final String temp : paths) {
            try {
                response = Optional.ofNullable(getZendeskRestClient().doGet(temp, task, true));

                // in guessing, break when we have data
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

    private List<String> getListPathByTarget()
    {
        return task.getTarget().equals(Target.OBJECT_RECORDS)
                ? task.getObjectTypes().stream().map(this::buildPath).collect(Collectors.toList())
                : task.getRelationshipTypes().stream().map(this::buildPath).collect(Collectors.toList());
    }

    private String buildPath(final String value)
    {
        final String perPage = Exec.isPreview() ? "1" : "1000";

        return getURIBuilderFromHost()
                .setPath(task.getTarget().equals(Target.OBJECT_RECORDS)
                        ? ZendeskConstants.Url.API_OBJECT_RECORD
                        : ZendeskConstants.Url.API_RELATIONSHIP_RECORD)
                .setParameter("type", value)
                .setParameter("per_page", perPage)
                .toString();
    }
}
