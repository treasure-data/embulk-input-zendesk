package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import java.util.List;

public class ZendeskDateUtils
{
    private static String supportedTimeFormat(final String value, final List<String> supportedFormats)
    {
        for (final String fmt : supportedFormats) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fmt);
            try {
                formatter.parse(value);
                return fmt;
            }
            catch (final DateTimeParseException e) {
                // Do nothing
            }
        }
        return "";
    }

    private ZendeskDateUtils()
    {
    }

    public static long isoToEpochSecond(final String time)
    {
        String pattern = supportedTimeFormat(time, Arrays.asList(ZendeskConstants.Misc.ISO_INSTANT,
                ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT));
        if (!pattern.isEmpty()) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
            final OffsetDateTime offsetDateTime = LocalDateTime.parse(time, formatter).atOffset(ZoneOffset.UTC);
            return offsetDateTime.toInstant().getEpochSecond();
        }
        else {
            throw new DataException("Fail to parse value '" + time + "' follow format " + DateTimeFormatter.ISO_INSTANT.toString());
        }
    }
}
