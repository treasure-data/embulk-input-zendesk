package org.embulk.input.zendesk.stream.paginator.support;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;

public class UserSpliterator extends SupportSpliterator
{
    public UserSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(Target.USERS, path, zendeskRestClient, task, isPreview);
    }
}
