package org.embulk.input.zendesk.utils;

import com.google.common.base.Joiner;
import org.embulk.config.ConfigException;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

public class ZendeskDateUtils
{
    public static void parse(final String value, final List<String> supportedFormats)
            throws ConfigException
    {
        for (final String fmt : supportedFormats) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fmt).withZone(ZoneOffset.UTC);
            try {
                ZonedDateTime.parse(value, formatter);
                return;
            }
            catch (final DateTimeParseException e) {
                // Do nothing
            }
        }
        throw new ConfigException("Unsupported DateTime value: '" + value + "', supported formats: [" + Joiner.on(",").join(supportedFormats) + "]");
    }

    private ZendeskDateUtils()
    {
    }

    public static long toTimeStamp(final String time)
    {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(time);
        offsetDateTime.format(DateTimeFormatter.ISO_INSTANT);
        return offsetDateTime.atZoneSameInstant(ZoneOffset.UTC).toInstant().getEpochSecond();
    }
}
