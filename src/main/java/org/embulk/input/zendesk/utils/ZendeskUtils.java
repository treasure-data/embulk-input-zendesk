package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.embulk.input.zendesk.models.Target;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZendeskUtils
{
    private ZendeskUtils()
    {}

    public static boolean isSupportIncremental(final Target target)
    {
        return !(target.equals(Target.TICKET_FORMS) || target.equals(Target.TICKET_FIELDS));
    }

    // For more information: https://developer.zendesk.com/rest_api/docs/support/side_loading#supported-endpoints
    public static boolean isSupportInclude(final Target target)
    {
        return target == Target.TICKETS
                || target == Target.USERS
                || target == Target.ORGANIZATIONS;
    }

    public static String convertBase64(final String text)
    {
        return Base64.encodeBase64String(text.getBytes(Charsets.UTF_8));
    }

    public static Type getColumnType(final String key, final JsonNode value)
    {
        Pattern patternTime = Pattern.compile(ZendeskConstants.Regex.TIME_FIELD);
        Matcher matcherTime = patternTime.matcher(key);
        if (value.isArray() || value.isObject()) {
            return Types.JSON;
        }
        else if (matcherTime.find()) {
            return Types.TIMESTAMP;
        }
        else {
            return Types.STRING;
        }
    }
}
