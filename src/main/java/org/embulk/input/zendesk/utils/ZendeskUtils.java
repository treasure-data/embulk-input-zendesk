package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.embulk.input.zendesk.models.Target;
import org.embulk.spi.DataException;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Iterator;

public class ZendeskUtils
{
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        ZendeskUtils.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ZendeskUtils.mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    private ZendeskUtils()
    {}

    public static boolean isSupportAPIIncremental(final Target target)
    {
        return !Target.TICKET_FORMS.equals(target)
                && !Target.TICKET_FIELDS.equals(target)
                && !Target.SCORES.equals(target)
                && !Target.RECIPIENTS.equals(target);
    }

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

    public static String convertToDateTimeFormat(String datetime, String dateTimeFormat)
    {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(ZendeskDateUtils.isoToEpochSecond(datetime)), ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern(dateTimeFormat));
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
