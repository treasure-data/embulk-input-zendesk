package org.embulk.input.zendesk.clients;

import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;

public interface ZendeskRestClient extends AutoCloseable
{
    <T> T doGet(final String url, final Jetty92ResponseReader<T> responseReader);

    @Override
    void close();
}
