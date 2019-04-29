package org.embulk.input.zendesk.stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpStatus;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.utils.ZendeskUtils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public abstract class PagingSpliterator<E> implements Spliterator<E>
{
    protected ZendeskRestClient zendeskRestClient;
    protected boolean isPreview;
    protected ZendeskInputPlugin.PluginTask task;
    protected String path;

    protected PagingSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        this.path = path;
        this.zendeskRestClient = zendeskRestClient;
        this.task = task;
        this.isPreview = isPreview;
    }

    @Override
    public Spliterator trySplit()
    {
        return null;
    }

    @Override
    public int characteristics()
    {
        return DISTINCT | NONNULL | IMMUTABLE;
    }

    @Override
    public long estimateSize()
    {
        return Long.MAX_VALUE;
    }

    protected boolean tryAdvanceSupportEndpoint(Target target, final Consumer<? super JsonNode> action)
    {
        final String result = zendeskRestClient.doGet(path, task, isPreview);
        if (result != null && !result.isEmpty()) {
            final JsonNode jsonNode = ZendeskUtils.parseJsonObject(result);
            final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(jsonNode, target.getJsonName());
            iterator.forEachRemaining(
                    item -> {
                        if (!item.isNull()) {
                            action.accept(item);
                        }
                    });
            if (jsonNode.get("next_page") != null && !jsonNode.get("next_page").isNull()) {
                path = jsonNode.get("next_page").asText();
                return true;
            }
        }

        return false;
    }

    protected boolean tryAdvanceSunshineEndpoint(final Consumer<? super JsonNode> action)
    {
        // For CustomObjects, when we don't have custom object type or relationship type matched with the searching text. API will return 404.
        // For UserEvent, when we don't find the result. API will return 404.
        // It should be ignored
        try {
            final String result = zendeskRestClient.doGet(path, task, isPreview);

            if (result != null && !result.isEmpty()) {
                final JsonNode jsonNode = ZendeskUtils.parseJsonObject(result);
                return isContinue(jsonNode, action);
            }
        }
        catch (final ConfigException e) {
            if (!(e.getCause() instanceof ZendeskException && ((ZendeskException) e.getCause()).getStatusCode() == HttpStatus.SC_NOT_FOUND)) {
                throw e;
            }
        }
        return false;
    }

    protected boolean isContinue(final JsonNode jsonNode, final Consumer<? super JsonNode> action)
    {
        final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(jsonNode, task.getTarget().getJsonName());

        // preview is only valid when running with Custom_Object targets because we mock data in User_Event target
        if (isPreview) {
            if (iterator.hasNext()) {
                JsonNode item = iterator.next();
                // we have data for preview, no need to continue
                if (!item.isNull()) {
                    action.accept(item);
                }
                return false;
            }
        }
        handleRunIterator(iterator, action);

        if (jsonNode.has("links") && !jsonNode.get("links").isNull()
                && jsonNode.get("links").has("next") && !jsonNode.get("links").get("next").isNull()) {
            path = task.getLoginUrl() + jsonNode.get("links").get("next");
            return true;
        }
        // escape only when isPreview and contain data for preview
        return false;
    }

    protected void handleRunIterator(Iterator<JsonNode> iterator,  final Consumer<? super JsonNode> action)
    {
        iterator.forEachRemaining(
                item -> {
                    if (!item.isNull()) {
                        action.accept(iterator.next());
                    }
                });
    }
}
