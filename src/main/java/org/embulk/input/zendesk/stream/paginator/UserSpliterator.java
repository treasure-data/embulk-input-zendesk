package org.embulk.input.zendesk.stream.paginator;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.stream.PagingSpliterator;

import java.util.function.Consumer;

public class UserSpliterator extends PagingSpliterator<JsonNode>
{
    public UserSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super JsonNode> action)
    {
        return tryAdvanceSupportEndpoint(Target.USERS, action);
    }
}
