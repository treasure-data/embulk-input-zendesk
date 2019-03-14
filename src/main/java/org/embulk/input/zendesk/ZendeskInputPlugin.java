package org.embulk.input.zendesk;

import org.embulk.base.restclient.RestClientInputPluginBase;

public class ZendeskInputPlugin
        extends RestClientInputPluginBase<ZendeskInputPluginDelegate.PluginTask>
{
    public ZendeskInputPlugin()
    {
        super(ZendeskInputPluginDelegate.PluginTask.class, new ZendeskInputPluginDelegate());
    }
}
