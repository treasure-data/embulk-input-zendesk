package org.embulk.input.zendesk.stream.paginator.support;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.stream.PagingSpliterator;
import org.embulk.input.zendesk.utils.ZendeskUtils;

import java.util.Iterator;
import java.util.function.Consumer;

public class SupportSpliterator extends PagingSpliterator<JsonNode>
{
    Target target;

    public SupportSpliterator(final Target target, final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
        this.target = target;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super JsonNode> action)
    {
        final String result = zendeskRestClient.doGet(path, task, isPreview);
        if (result != null && !result.isEmpty()) {
            final JsonNode jsonNode = ZendeskUtils.parseJsonObject(result);
            final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(jsonNode, target.getJsonName());
            iterator.forEachRemaining(
                    item -> {
                        if (!ZendeskUtils.isNull(item)) {
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
}
