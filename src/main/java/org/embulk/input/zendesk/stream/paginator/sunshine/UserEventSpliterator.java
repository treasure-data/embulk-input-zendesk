package org.embulk.input.zendesk.stream.paginator.sunshine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;

public class UserEventSpliterator extends SunshineSpliterator
{
    private String userID;

    private static final Logger logger = LoggerFactory.getLogger(UserEventSpliterator.class);

    public UserEventSpliterator(final String userID, final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
        this.userID = userID;
    }

    @Override
    protected void handleRunIterator(final Iterator<JsonNode> iterator, final Consumer<? super JsonNode> action)
    {
        iterator.forEachRemaining(
            item -> {
                    if (!ZendeskUtils.isNull(item)) {
                        JsonNode temp = iterator.next();
                        // Because in the returned json doesn't have user_id, so we try to add to it
                        ((ObjectNode) temp).put("user_id", this.userID);
                        action.accept(temp);
                    }
                });
    }
}
