package org.embulk.input.zendesk.clients;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPluginDelegate;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZendeskRestClientImpl implements ZendeskRestClient
{
    private static final Logger logger = Exec.getLogger(ZendeskRestClientImpl.class);
    private static RateLimiter rateLimiter;

    private final Jetty92RetryHelper retryHelper;
    private final ZendeskInputPluginDelegate.PluginTask task;

    public ZendeskRestClientImpl(final Jetty92RetryHelper retryHelper, final ZendeskInputPluginDelegate.PluginTask task)
    {
        this.retryHelper = retryHelper;
        this.task = task;
    }

    public <T> T doGet(final String url, final Jetty92ResponseReader<T> responseReader)
    {
        T result;
        if (rateLimiter == null) {
            result = retryHelper.requestWithRetry(responseReader, new ZendeskV2Jetty92SingleRequester(url, buildAuthHeader()));
            try {
                initRateLimiter(Double.parseDouble(responseReader.getResponse().getHeaders().get("x-rate-limit")), task);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            rateLimiter.acquire();
            result = retryHelper.requestWithRetry(responseReader, new ZendeskV2Jetty92SingleRequester(url, buildAuthHeader()));
        }
        return result;
    }

    @Override
    public void close()
    {
        retryHelper.close();
    }

    private static class ZendeskV2Jetty92SingleRequester extends Jetty92SingleRequester
    {
        private final String url;
        private final ImmutableMap<String, String> headers;

        ZendeskV2Jetty92SingleRequester(final String url, final ImmutableMap<String, String> headers)
        {
            this.url = url;
            this.headers = headers;
        }

        @Override
        public void requestOnce(final HttpClient client, final Response.Listener responseListener)
        {
            final Request request = client.newRequest(url).method(HttpMethod.GET);

            if (headers != null) {
                for (final Map.Entry<String, String> entry : headers.entrySet()) {
                    request.header(entry.getKey(), entry.getValue());
                }
            }

            logger.info(">>> {} {}{}", request.getMethod(), request.getURI().getPath(),
                    request.getURI().getQuery() != null ? "?" + request.getURI().getQuery() : "");
            request.send(responseListener);
        }

        /**
         * Analyze response whether the failure response is retryable or not
         *
         * Only retry if reaches the API limitation
         */
        @Override
        protected boolean isResponseStatusToRetry(final Response response)
        {
            int status = response.getStatus();
            String retryAfter;
            switch (status) {
                case 409:
                case 422:
                    return true;
                case 429:
                    retryAfter = response.getHeaders().get("Retry-After");

                    if (!retryAfter.isEmpty()) {
                        logger.warn("Reached API limitation, sleep for {} {}", retryAfter,
                                TimeUnit.SECONDS.name());
                        Uninterruptibles.sleepUninterruptibly(Long.parseLong(retryAfter), TimeUnit.SECONDS);
                    }
                    return true;
                case 503:
                    retryAfter = response.getHeaders().get("Retry-After");
                    if (!retryAfter.isEmpty()) {
                        logger.warn("Reached API limitation, sleep for {} {}", retryAfter,
                                TimeUnit.SECONDS.name());
                        Uninterruptibles.sleepUninterruptibly(Long.parseLong(retryAfter), TimeUnit.SECONDS);
                    }
                    else {
                        throw new RuntimeException(String.format("%s temporally failure.", status));
                    }
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Analyze exception whether retryable or not
         */
        @Override
        protected boolean isExceptionToRetry(final Exception exception)
        {
            if (exception instanceof ConfigException || exception instanceof ExecutionException) {
                return toRetry((Exception) exception.getCause());
            }
            return exception instanceof TimeoutException || super.isExceptionToRetry(exception);
        }
    }

    private ImmutableMap buildAuthHeader()
    {
        switch (task.getAuthenticationMethod()) {
            case BASIC:
                return ImmutableMap.builder()
                        .put(ZendeskConstants.Header.AUTHORIZATION, "Basic " +
                                ZendeskUtils.convertBase64(String.format("%s:%s", task.getUsername().get(),
                                        task.getPassword().get())))
                        .putAll(buildCommonHeader())
                        .build();
            case TOKEN:
                return ImmutableMap.builder()
                        .put(ZendeskConstants.Header.AUTHORIZATION, "Basic " +
                                ZendeskUtils.convertBase64(String.format("%s/token:%s", task.getUsername().get(),
                                        task.getToken().get())))
                        .putAll(buildCommonHeader())
                        .build();
            case OAUTH:
                return ImmutableMap.builder()
                        .put(ZendeskConstants.Header.AUTHORIZATION, "Bearer " + task.getAccessToken().get())
                        .putAll(buildCommonHeader())
                        .build();
            default:
                throw new ConfigException("Unsupported authentication method");
        }
    }

    private ImmutableMap buildCommonHeader()
    {
        ImmutableMap.Builder builder = ImmutableMap.<String, String>builder();

        if (task.getAppMarketPlaceIntegrationName().isPresent()) {
            builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_NAME, task.getAppMarketPlaceIntegrationName().get());
        }
        if (task.getAppMarketPlaceOrgId().isPresent()) {
            builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_ORGANIZATION_ID, task.getAppMarketPlaceOrgId().get());
        }
        if (task.getAppMarketPlaceAppId().isPresent()) {
            builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_APP_ID, task.getAppMarketPlaceAppId().get());
        }

        builder.put(ZendeskConstants.Header.CONTENT_TYPE, ZendeskConstants.Header.APPLICATION_JSON);
        return builder.build();
    }

    private static synchronized void initRateLimiter(double permits, final ZendeskInputPluginDelegate.PluginTask task)
    {
        if (rateLimiter == null) {
            permits = (task.getIncremental())
                    ? permits - 1
                    : (permits - 1) / 60;

            rateLimiter = RateLimiter.create(permits);
        }
    }
}
