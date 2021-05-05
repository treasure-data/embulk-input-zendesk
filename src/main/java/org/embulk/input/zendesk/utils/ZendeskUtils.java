package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Iterator;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class ZendeskUtils
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = Exec.getLogger(ZendeskUtils.class);

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    private ZendeskUtils()
    {}

    public static String convertBase64(final String text)
    {
        return Base64.getEncoder().encodeToString(text.getBytes(Charsets.UTF_8));
    }

    public static int numberToSplitWithHintingInTask(final int count)
    {
        return (int) Math.ceil((double) count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE);
    }

    public static ObjectNode parseJsonObject(final String jsonText)
    {
        final JsonNode node = ZendeskUtils.parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        }

        throw new DataException("Expected object node to parse but doesn't get");
    }

    public static Iterator<JsonNode> getListRecords(final JsonNode result, final String targetJsonName)
    {
        if (!result.has(targetJsonName) || !result.get(targetJsonName).isArray()) {
            throw new DataException(String.format("Missing '%s' from Zendesk API response", targetJsonName));
        }
        return result.get(targetJsonName).elements();
    }

    public static boolean isNull(final JsonNode jsonNode)
    {
        return jsonNode == null || jsonNode.isNull();
    }

    public static URIBuilder getURIBuilder(final String urlString)
    {
        final URI uri;
        try {
            uri = new URI(urlString);
        }
        catch (final URISyntaxException e) {
            throw new ConfigException("URL is invalid format " + urlString);
        }

        return new URIBuilder()
                .setScheme(uri.getScheme())
                .setHost(uri.getHost());
    }

    private static JsonNode parseJsonNode(final String jsonText)
    {
        try {
            return ZendeskUtils.mapper.readTree(jsonText);
        }
        catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
