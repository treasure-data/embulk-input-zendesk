package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import java.util.List;

public class ZendeskDateUtils
{
    public static boolean isSupportedTimeFormat(final String value, final List<String> supportedFormats)
    {
        for (final String fmt : supportedFormats) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fmt);
            try {
                formatter.parse(value);
                return true;
            }
            catch (final DateTimeParseException e) {
                // Do nothing
            }
        }
        return false;
    }

    private ZendeskDateUtils()
    {
    }

    public static long isoToEpochSecond(final String time)
    {
        if (isSupportedTimeFormat(time, Arrays.asList(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT, ZendeskConstants.Misc.ISO_INSTANT))) {
            final OffsetDateTime offsetDateTime = OffsetDateTime.parse(time);
            offsetDateTime.format(DateTimeFormatter.ISO_INSTANT);
            return offsetDateTime.toInstant().getEpochSecond();
        }
        else {
            throw new DataException("Fail to parse value '" + time + "' follow format " + DateTimeFormatter.ISO_INSTANT.toString());
        }
    }
}
