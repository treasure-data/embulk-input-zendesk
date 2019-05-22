package org.embulk.input.zendesk.stream.paginator.support;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.Target;

public class OrganizationSpliterator extends SupportSpliterator
{
    public OrganizationSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task)
    {
        super(Target.ORGANIZATIONS, path, zendeskRestClient, task, false);
    }
}
