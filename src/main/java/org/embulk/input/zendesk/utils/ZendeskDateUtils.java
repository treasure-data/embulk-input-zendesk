package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.Optional;

public class ZendeskDateUtils
{
    private ZendeskDateUtils()
    {
    }

    public static long isoToEpochSecond(final String time)
    {
        final Optional<String> pattern = supportedTimeFormat(time);
        if (pattern.isPresent()) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern.get()).withZone(ZoneOffset.UTC);
            final OffsetDateTime offsetDateTime = LocalDateTime.parse(time, formatter).atOffset(ZoneOffset.UTC);
            return offsetDateTime.toInstant().getEpochSecond();
        }

        throw new DataException("Fail to parse value '" + time + "' follow formats " + ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT.toString());
    }

    public static Optional<String> supportedTimeFormat(final String value)
    {
        for (final String fmt : ZendeskConstants.Misc.SUPPORT_DATE_TIME_FORMAT) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fmt);
            try {
                formatter.parse(value);
                return Optional.of(fmt);
            }
            catch (final DateTimeParseException e) {
                // Do nothing
            }
        }
        return Optional.empty();
    }

    public static String convertToDateTimeFormat(String datetime, String dateTimeFormat)
    {
        return Instant.ofEpochSecond(ZendeskDateUtils.isoToEpochSecond(datetime)).atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(dateTimeFormat));
    }

    // start_time should be start from 0
    public static long getStartTime(final  String time)
    {
        try {
            return isoToEpochSecond(time);
        }
        catch (DataException ex) {
            return 0;
        }
    }
}
