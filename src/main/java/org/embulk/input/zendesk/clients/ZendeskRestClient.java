package org.embulk.input.zendesk.clients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.embulk.config.ConfigException;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.protocol.HTTP.CONTENT_TYPE;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

public class ZendeskRestClient
{
    private static final int CONNECTION_TIME_OUT = 240000;

    private static final Logger logger = Exec.getLogger(ZendeskRestClient.class);

    private static RateLimiter rateLimiter;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public ZendeskRestClient()
    {
    }

    public String doGet(String url, PluginTask task, boolean isPreview)
    {
        try {
            return retryExecutor().withRetryLimit(task.getRetryLimit())
                    .withInitialRetryWait(task.getRetryInitialWaitSec() * 1000)
                    .withMaxRetryWait(task.getMaxRetryWaitSec() * 1000)
                    .runInterruptible(new RetryExecutor.Retryable<String>() {
                        @Override
                        public String call() throws Exception
                        {
                            return sendGetRequest(url, task);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (exception instanceof ZendeskException) {
                                int statusCode = ((ZendeskException) exception).getStatusCode();
                                return isResponseStatusToRetry(statusCode, exception.getMessage(), ((ZendeskException) exception).getRetryAfter(), isPreview);
                            }
                            return false;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                        {
                            if (exception instanceof ZendeskException) {
                                int retryAfter = ((ZendeskException) exception).getRetryAfter();
                                String message;
                                if (retryAfter > 0 && retryAfter > (retryWait / 1000)) {
                                    message = String
                                            .format("Retrying '%d'/'%d' after '%d' seconds. HTTP status code: '%s'",
                                                    retryCount, retryLimit,
                                                    retryAfter,
                                                    ((ZendeskException) exception).getStatusCode());
                                    logger.warn(message);
                                    Uninterruptibles.sleepUninterruptibly(retryAfter - (retryWait / 1000), TimeUnit.SECONDS);
                                }
                                else {
                                    message = String
                                            .format("Retrying '%d'/'%d' after '%d' seconds. HTTP status code: '%s'",
                                                    retryCount, retryLimit,
                                                    retryWait / 1000,
                                                    ((ZendeskException) exception).getStatusCode());
                                    logger.warn(message);
                                }
                            }
                            else {
                                String message = String
                                        .format("Retrying '%d'/'%d' after '%d' seconds. Message: '%s'",
                                                retryCount, retryLimit,
                                                retryWait / 1000,
                                                exception.getMessage());
                                logger.warn(message, exception);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                        {
                        }
                    });
        }
        catch (RetryExecutor.RetryGiveupException | InterruptedException e) {
            if (e instanceof RetryExecutor.RetryGiveupException && e.getCause() != null && e.getCause() instanceof ZendeskException) {
                throw new ConfigException(e.getCause().getMessage());
            }
            throw new ConfigException(e);
        }
    }

    @VisibleForTesting
    protected HttpClient createHttpClient()
    {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(CONNECTION_TIME_OUT)
                .setConnectionRequestTimeout(CONNECTION_TIME_OUT)
                .build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    private String sendGetRequest(final String url, final PluginTask task) throws ZendeskException
    {
        try {
            HttpClient client = createHttpClient();
            HttpRequestBase request = createGetRequest(url, task);
            logger.info(">>> {}{}", request.getURI().getPath(),
                    request.getURI().getQuery() != null ? "?" + request.getURI().getQuery() : "");
            HttpResponse response = client.execute(request);
            getRateLimiter(response).acquire();
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                if (statusCode == ZendeskConstants.HttpStatus.TOO_MANY_REQUEST || statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR
                        || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                    Header retryHeader = response.getFirstHeader("Retry-After");
                    if (retryHeader != null) {
                        throw new ZendeskException(statusCode, EntityUtils.toString(response.getEntity()), Integer.parseInt(retryHeader.getValue()));
                    }
                }
                throw new ZendeskException(statusCode, EntityUtils.toString(response.getEntity()), 0);
            }
            return EntityUtils.toString(response.getEntity());
        }
        catch (IOException ex) {
            throw new ZendeskException(-1, ex.getMessage(), 0);
        }
    }

    private boolean isResponseStatusToRetry(final int status, final String message, int retryAfter, final boolean isPreview)
    {
        if (status == HttpStatus.SC_NOT_FOUND) {
            // 404 would be returned e.g. ticket comments are empty (on fetchRelatedObjects method)
            return false;
        }

        if (status == HttpStatus.SC_CONFLICT) {
            logger.warn(String.format("'%s' temporally failure.", status));
            return true;
        }

        if (status == HttpStatus.SC_UNPROCESSABLE_ENTITY) {
            if (message.startsWith(ZendeskConstants.Misc.TOO_RECENT_START_TIME)) {
                //That means "No records from start_time". We can recognize it same as 200.
                return false;
            }
            throw new ConfigException("Status: '" + status + "', error message '" + message + "'");
        }

        if (status == ZendeskConstants.HttpStatus.TOO_MANY_REQUEST || status == HttpStatus.SC_INTERNAL_SERVER_ERROR
                || status == HttpStatus.SC_SERVICE_UNAVAILABLE) {
            if (!isPreview) {
                if (retryAfter > 0) {
                    logger.warn("Reached API limitation, wait for at least '{}' '{}'", retryAfter, TimeUnit.SECONDS.name());
                }
                else if (status != ZendeskConstants.HttpStatus.TOO_MANY_REQUEST) {
                    logger.warn(String.format("'%s' temporally failure.", status));
                }
                return true;
            }
            throw new DataException("Rate Limited. Waiting '" + retryAfter + "' seconds to re-run");
        }

        // Won't retry for 4xx range errors except above. Almost they should be ConfigError e.g. 403 Forbidden
        if (status / 100 == 4) {
            throw new ConfigException("Status '" + status + "', message '" + message + "'");
        }

        logger.warn("Server returns unknown status code '" + status + "' message '" + message + "'");
        return true;
    }

    private HttpRequestBase createGetRequest(String url, PluginTask task)
    {
        HttpGet request = new HttpGet(url);
        ImmutableMap<String, String> headers = buildAuthHeader(task);
        headers.forEach(request::setHeader);
        return request;
    }

    private ImmutableMap<String, String> buildAuthHeader(PluginTask task)
    {
        Builder<String, String> builder = new Builder<>();
        builder.put(AUTHORIZATION, buildCredential(task));
        addCommonHeader(builder, task);
        return builder.build();
    }

    private String buildCredential(PluginTask task)
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

    private void addCommonHeader(final Builder<String, String> builder, PluginTask task)
    {
        task.getAppMarketPlaceIntegrationName().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_NAME, s));
        task.getAppMarketPlaceAppId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_APP_ID, s));
        task.getAppMarketPlaceOrgId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_ORGANIZATION_ID, s));

        builder.put(CONTENT_TYPE, ZendeskConstants.Header.APPLICATION_JSON);
    }

    private RateLimiter getRateLimiter(final HttpResponse response)
    {
        if (rateLimiter == null) {
            rateLimiter = initRateLimiter(response);
        }
        return rateLimiter;
    }

    private static synchronized RateLimiter initRateLimiter(final HttpResponse response)
    {
        String rateLimit = "";
        double permits = 0.0;
        try {
            if (response.getFirstHeader("x-rate-limit") != null) {
                rateLimit = response.getFirstHeader("x-rate-limit").getValue();
            }
            permits = Double.parseDouble(rateLimit);
        }
        catch (NumberFormatException e) {
            throw new DataException("Error when parse x-rate-limit: '" + response.getFirstHeader("x-rate-limit").getValue() + "'");
        }
        permits = permits / 60;
        logger.info("Permits per second " + permits);

        return RateLimiter.create(permits);
    }
}
