package org.embulk.input.zendesk.stream.paginator.sunshine;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;

public class CustomObjectSpliterator extends SunshineSpliterator
{
    public CustomObjectSpliterator(final String path, final ZendeskRestClient zendeskRestClient, final ZendeskInputPlugin.PluginTask task, final boolean isPreview)
    {
        super(path, zendeskRestClient, task, isPreview);
    }
}
