package org.embulk.input.zendesk.stream;

import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;

import java.util.Spliterator;

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
    public Spliterator<E> trySplit()
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
}
