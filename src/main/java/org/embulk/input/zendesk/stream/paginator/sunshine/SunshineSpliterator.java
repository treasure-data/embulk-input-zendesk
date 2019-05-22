package org.embulk.input.zendesk.stream.paginator.sunshine;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpStatus;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.stream.PagingSpliterator;
import org.embulk.input.zendesk.utils.ZendeskUtils;

import java.util.Iterator;
import java.util.function.Consumer;

public class SunshineSpliterator extends PagingSpliterator<JsonNode>
{
    public SunshineSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super JsonNode> action)
    {
        try {
            final String result = zendeskRestClient.doGet(path, task, isPreview);

            if (result != null && !result.isEmpty()) {
                final JsonNode jsonNode = ZendeskUtils.parseJsonObject(result);
                final JsonNode targetJsonNode = jsonNode.get(task.getTarget().getJsonName());
                if (!ZendeskUtils.isNull(targetJsonNode)) {
                    return isContinue(jsonNode, action);
                }
            }
        }
        catch (final ConfigException e) {
            if (!(e.getCause() instanceof ZendeskException && ((ZendeskException) e.getCause()).getStatusCode() == HttpStatus.SC_NOT_FOUND)) {
                throw e;
            }
        }
        return false;
    }

    protected void handleRunIterator(Iterator<JsonNode> iterator,  final Consumer<? super JsonNode> action)
    {
        iterator.forEachRemaining(
                item -> {
                    if (item != null && !item.isNull()) {
                        action.accept(iterator.next());
                    }
                });
    }

    protected boolean isContinue(final JsonNode jsonNode, final Consumer<? super JsonNode> action)
    {
        final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(jsonNode, task.getTarget().getJsonName());
        handleRunIterator(iterator, action);

        if (jsonNode.has("links") && !ZendeskUtils.isNull(jsonNode.get("links"))
                && jsonNode.get("links").has("next") && !ZendeskUtils.isNull(jsonNode.get("links").get("next"))) {
            path = task.getLoginUrl() + jsonNode.get("links").get("next");
            return true;
        }
        return false;
    }
}
