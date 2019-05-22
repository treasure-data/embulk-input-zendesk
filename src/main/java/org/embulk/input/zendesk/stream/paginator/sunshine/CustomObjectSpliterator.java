package org.embulk.input.zendesk.stream.paginator.sunshine;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskUtils;

import java.util.Iterator;
import java.util.function.Consumer;

public class CustomObjectSpliterator extends SunshineSpliterator
{
    public CustomObjectSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
    }

    @Override
    protected boolean isContinue(final JsonNode jsonNode, final Consumer<? super JsonNode> action)
    {
        final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(jsonNode, task.getTarget().getJsonName());

        if (isPreview) {
            if (iterator.hasNext()) {
                JsonNode item = iterator.next();
                // we have data for preview, no need to continue
                if (item != null && !item.isNull()) {
                    action.accept(item);
                    return false;
                }
            }
        }
        handleRunIterator(iterator, action);

        if (jsonNode.has("links") && !ZendeskUtils.isNull(jsonNode.get("links"))
                && jsonNode.get("links").has("next") && !ZendeskUtils.isNull(jsonNode.get("links").get("next"))) {
            path = task.getLoginUrl() + jsonNode.get("links").get("next");
            return true;
        }
        return false;
    }
}
