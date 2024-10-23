package org.embulk.input.zendesk.clients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.protocol.HTTP.CONTENT_TYPE;

public class ZendeskRestClient
{
    private static final int CONNECTION_TIME_OUT = 240000;

    private static final Logger logger = LoggerFactory.getLogger(ZendeskRestClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static RateLimiter rateLimiter;
    private Target target;
    private String loginURL;

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public ZendeskRestClient()
    {
    }

    public String doGet(final String url, final PluginTask task, final boolean isPreview)
    {
        target = task.getTarget();
        loginURL = task.getLoginUrl();

        try {
            return RetryExecutor.builder()
                .withRetryLimit(task.getRetryLimit())
                .withInitialRetryWaitMillis(task.getRetryInitialWaitSec() * 1000)
                .withMaxRetryWaitMillis(task.getMaxRetryWaitSec() * 1000)
                .build()
                .runInterruptible(new Retryable<String>() {
                @Override
                public String call()
                    throws Exception
                {
                    return sendGetRequest(url, task);
                }

                @Override
                public boolean isRetryableException(final Exception exception)
                {
                    if (exception instanceof ZendeskException) {
                        final int statusCode = ((ZendeskException) exception).getStatusCode();
                        return isResponseStatusToRetry(statusCode, exception.getMessage(), ((ZendeskException) exception).getRetryAfter(), isPreview);
                    }
                    return false;
                }

                @Override
                public void onRetry(final Exception exception, final int retryCount, final int retryLimit, final int retryWait)
                {
                    if (exception instanceof ZendeskException) {
                        final int retryAfter = ((ZendeskException) exception).getRetryAfter();
                        final String message;
                        if (retryAfter > 0 && retryAfter > (retryWait / 1000)) {
                            message = String.format("Retrying '%d'/'%d' after '%d' seconds. HTTP status code: '%s'", retryCount, retryLimit, retryAfter, ((ZendeskException) exception).getStatusCode());
                            logger.warn(message);
                            Uninterruptibles.sleepUninterruptibly(retryAfter - (retryWait / 1000), TimeUnit.SECONDS);
                        }
                        else {
                            message = String.format("Retrying '%d'/'%d' after '%d' seconds. HTTP status code: '%s'", retryCount, retryLimit, retryWait / 1000, ((ZendeskException) exception).getStatusCode());
                            logger.warn(message);
                        }
                    }
                    else {
                        final String message = String.format("Retrying '%d'/'%d' after '%d' seconds. Message: '%s'", retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                        logger.warn(message, exception);
                    }
                }

                @Override
                public void onGiveup(final Exception firstException, final Exception lastException)
                {
                }
            });
        }
        catch (final RetryGiveupException | InterruptedException e) {
            if (e instanceof RetryGiveupException && e.getCause() != null && e.getCause() instanceof ZendeskException) {
                throw new ConfigException("Status: '" + ((ZendeskException) (e.getCause())).getStatusCode() + "', error message: '" + e.getCause().getMessage() + "'", e.getCause());
            }
            throw new ConfigException(e);
        }
    }

    @VisibleForTesting
    protected HttpClient createHttpClient()
    {
        final RequestConfig config = RequestConfig.custom().setConnectTimeout(CONNECTION_TIME_OUT).setConnectionRequestTimeout(CONNECTION_TIME_OUT).build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    private String sendGetRequest(final String url, final PluginTask task)
        throws ZendeskException
    {
        try {
            final HttpClient client = createHttpClient();
            final HttpRequestBase request = createGetRequest(url, task);

            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            logger.info(">>> {}{}", request.getURI().getPath(), request.getURI().getQuery() != null ? "?" + request.getURI().getQuery() : "");
            final HttpResponse response = client.execute(request);
            if (rateLimiter == null) {
                initRateLimiter(response);
            }
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                if (statusCode == ZendeskConstants.HttpStatus.TOO_MANY_REQUEST || statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                    final Header retryHeader = response.getFirstHeader("Retry-After");
                    if (retryHeader != null) {
                        throw new ZendeskException(statusCode, EntityUtils.toString(response.getEntity()), Integer.parseInt(retryHeader.getValue()));
                    }
                }
                throw new ZendeskException(statusCode, EntityUtils.toString(response.getEntity()), 0);
            }
            return EntityUtils.toString(response.getEntity());
        }
        catch (final IOException ex) {
            throw new ZendeskException(-1, ex.getMessage(), 0);
        }
    }

    private boolean isResponseStatusToRetry(final int status, final String message, final int retryAfter, final boolean isPreview)
    {
        if (status == HttpStatus.SC_NOT_FOUND) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode jsonNode = objectMapper.readTree(message);
                if (jsonNode.has("error") && jsonNode.get("error").has("title") && jsonNode.get("error").get("title").asText().startsWith("No help desk at ")) {
                    throw new ConfigException("This address is not registered in Zendesk. Please check the login_url again");
                }
            }
            catch (IOException e) {
                // In case we can't parse the message, error should not be show here
            }

            if (!Pattern.compile(ZendeskConstants.Regex.LOGIN_URL).matcher(loginURL).matches()) {
                throw new ConfigException("Invalid login url. Check that you are using the correct Zendesk url (https://example.zendesk.com/) to import data.");
            }

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
            throw new ConfigException("Status: '" + status + "', error message: '" + message + "'");
        }

        if (status == ZendeskConstants.HttpStatus.TOO_MANY_REQUEST || status == HttpStatus.SC_INTERNAL_SERVER_ERROR || status == HttpStatus.SC_SERVICE_UNAVAILABLE) {
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
            if (status == HttpStatus.SC_UNAUTHORIZED) {
                throw new ConfigException("Invalid credentials. Check that you are using your Zendesk credentials.");
            }

            if (status == HttpStatus.SC_FORBIDDEN) {
                throw new ConfigException("You do not have access to this resource. Please contact the account owner of this help desk for further help.");
            }

            throw new ConfigException("Status '" + status + "', message '" + message + "'");
        }

        logger.warn("Server returns unknown status code '" + status + "' message '" + message + "'");
        return true;
    }

    private HttpRequestBase createGetRequest(final String url, final PluginTask task)
    {
        final HttpGet request = new HttpGet(url);
        final ImmutableMap<String, String> headers = buildAuthHeader(task);
        headers.forEach(request::setHeader);
        return request;
    }

    private ImmutableMap<String, String> buildAuthHeader(final PluginTask task)
    {
        final Builder<String, String> builder = new Builder<>();
        builder.put(AUTHORIZATION, buildCredential(task));
        addCommonHeader(builder, task);
        return builder.build();
    }

    private String buildCredential(final PluginTask task)
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

    private void addCommonHeader(final Builder<String, String> builder, final PluginTask task)
    {
        task.getAppMarketPlaceIntegrationName().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_NAME, s));
        task.getAppMarketPlaceAppId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_APP_ID, s));
        task.getAppMarketPlaceOrgId().ifPresent(s -> builder.put(ZendeskConstants.Header.ZENDESK_MARKETPLACE_ORGANIZATION_ID, s));

        builder.put(CONTENT_TYPE, ZendeskConstants.Header.APPLICATION_JSON);
    }

    private synchronized void initRateLimiter(final HttpResponse response)
    {
        if (response.containsHeader("x-rate-limit")) {
            double permits = 0.0;
            final String rateLimit = response.getFirstHeader("x-rate-limit").getValue();
            try {
                permits = Double.parseDouble(rateLimit);
            }
            catch (final NumberFormatException e) {
                throw new DataException("Error when parse x-rate-limit: '" + response.getFirstHeader("x-rate-limit").getValue() + "'");
            }

            if (permits > 0) {
                permits = permits / 60;
                logger.info("Permits per second " + permits);

                rateLimiter = RateLimiter.create(permits);
            }
        }
    }
}
