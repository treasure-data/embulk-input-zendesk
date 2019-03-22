package org.embulk.input.zendesk.clients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.ZendeskErrorResponse;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ZendeskRestClient implements AutoCloseable
{
    private static final Logger logger = Exec.getLogger(ZendeskRestClient.class);
    private static RateLimiter rateLimiter;

    private final Jetty92RetryHelper retryHelper;
    private final ZendeskInputPlugin.PluginTask task;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public ZendeskRestClient(final Jetty92RetryHelper retryHelper, final ZendeskInputPlugin.PluginTask task)
    {
        this.retryHelper = retryHelper;
        this.task = task;
    }

    public <T> T doGet(final String url, final Jetty92ResponseReader<T> responseReader)
    {
        if (rateLimiter != null) {
            getRateLimiter().acquire();
        }

        T result = retryHelper.requestWithRetry(responseReader, new Jetty92SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                final Request request = client.newRequest(url).method(HttpMethod.GET);
                ImmutableMap<String, String> headers = buildAuthHeader();
                if (headers != null) {
                    for (final Map.Entry<String, String> entry : headers.entrySet()) {
                        request.header(entry.getKey(), entry.getValue());
                    }
                }

                logger.info(">>> {} {}{}", request.getMethod(), request.getURI().getPath(),
                        request.getURI().getQuery() != null ? "?" + request.getURI().getQuery() : "");
                request.send(responseListener);
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                final int status = response.getStatus();
                if (status == 409) {
                    throw new RuntimeException(String.format("'%s' temporally failure.", status));
                }

                if (status == 422) {
                    ZendeskErrorResponse zendeskErrorResponse;
                    try {
                        zendeskErrorResponse = objectMapper.readValue(responseReader.readResponseContentInString(), ZendeskErrorResponse.class);
                    }
                    catch (final Exception e) {
                            throw new RuntimeException("Fail to parse body of response, error status '" + status + "'");
                    }
                    if (zendeskErrorResponse.description != null
                                && zendeskErrorResponse.description.startsWith(ZendeskConstants.Misc.TOO_RECENT_START_TIME)) {
                        return true;
                    }
                    else {
                        throw new ConfigException("Status: '" + status + "', error message +'" + zendeskErrorResponse.toString());
                    }
                }

                if (status == 429 || status == 500 || status == 503) {
                    final String retryAfter = response.getHeaders().get("Retry-After");
                    if (!retryAfter.isEmpty()) {
                        logger.warn("Reached API limitation, sleep for '{}' '{}'", retryAfter, TimeUnit.SECONDS.name());
                        Uninterruptibles.sleepUninterruptibly(Long.parseLong(retryAfter), TimeUnit.SECONDS);
                        return true;
                    }
                    else if (status != 429) {
                        throw new RuntimeException(String.format("'%s' temporally failure.", status));
                    }
                }

                // for 500s error we should retry
                if ((status / 100) == 5) {
                    return true;
                }
                else {
                    throw new RuntimeException("Server returns unknown status code '" + status + "'");
                }
            }

            //Analyze exception whether retryable or not
            @Override
            protected boolean isExceptionToRetry(final Exception exception)
            {
                if (exception instanceof ConfigException || exception instanceof ExecutionException) {
                    return toRetry((Exception) exception.getCause());
                }
                return exception instanceof IOException;
            }
        });

        if (rateLimiter == null) {
            initRateLimiter(responseReader);
        }

        return result;
    }

    @Override
    public void close()
    {
        retryHelper.close();
    }

    private ImmutableMap<String, String> buildAuthHeader()
    {
        Builder<String, String> builder = new Builder<>();
        builder.put(ZendeskConstants.Header.AUTHORIZATION, buildCredential());
        addCommonHeader(builder);
        return builder.build();
    }

    private String buildCredential()
    {
        switch (task.getAuthenticationMethod()) {
            case BASIC:
                return "Basic " + ZendeskUtils.convertBase64(String.format("%s:%s", task.getUsername().get(), task.getPassword().get()));
            case TOKEN:
                return "Basic " + ZendeskUtils.convertBase64(String.format("%s/token:%s", task.getUsername().get(), task.getToken().get()));
            case OAUTH:
                return "Bearer " + task.getAccessToken().get();
        }
        return "";
    }

    private void addCommonHeader(final Builder<String, String> builder)
    {
        task.getAppMarketPlaceIntegrationName().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_NAME, s));
        task.getAppMarketPlaceAppId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_APP_ID, s));
        task.getAppMarketPlaceOrgId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_ORGANIZATION_ID, s));

        builder.put(ZendeskConstants.Header.CONTENT_TYPE, ZendeskConstants.Header.APPLICATION_JSON);
    }

    private RateLimiter getRateLimiter()
    {
        return rateLimiter;
    }

    private static synchronized void initRateLimiter(final Jetty92ResponseReader responseReader)
    {
        String rateLimit = "";
        double permits = 0.0;
        try {
            rateLimit = responseReader.getResponse().getHeaders().get("x-rate-limit");
            permits = Double.parseDouble(rateLimit);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        permits = permits / 60;
        logger.info("Permits per second " + permits);

        rateLimiter = RateLimiter.create(permits);
    }
}
