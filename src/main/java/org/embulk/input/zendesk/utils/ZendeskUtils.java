package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.embulk.input.zendesk.models.Target;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZendeskUtils
{
    private static Pattern patternTime = Pattern.compile(ZendeskConstants.Regex.TIME_FIELD);

    private ZendeskUtils()
    {}

    public static boolean isSupportIncremental(final Target target)
    {
        return !Target.TICKET_FORMS.equals(target) && !Target.TICKET_FIELDS.equals(target);
    }

    // For more information: https://developer.zendesk.com/rest_api/docs/support/side_loading#supported-endpoints
    public static boolean isSupportInclude(final Target target)
    {
        return Target.TICKETS.equals(target)
                || Target.USERS.equals(target)
                || Target.ORGANIZATIONS.equals(target);
    }

    public static String convertBase64(final String text)
    {
        return Base64.encodeBase64String(text.getBytes(Charsets.UTF_8));
    }

    public static Type getColumnType(final String key, final JsonNode value)
    {
        if (value.isArray() || value.isObject()) {
            return Types.JSON;
        }

        Matcher matcherTime = patternTime.matcher(key);
        if (matcherTime.find()) {
            return Types.TIMESTAMP;
        }

        return Types.STRING;
    }

    public static boolean isSupportInclude(Target target, List<String> includes)
    {
        return includes != null && !includes.isEmpty() && ZendeskUtils.isSupportInclude(target);
    }
}
