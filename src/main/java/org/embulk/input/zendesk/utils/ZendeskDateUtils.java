package org.embulk.input.zendesk.utils;

import com.google.common.base.Joiner;
import org.embulk.spi.DataException;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import java.util.List;
import java.util.Optional;

public class ZendeskDateUtils
{
    private ZendeskDateUtils()
    {
    }

    private static final List<String> supportedFormats =  Arrays.asList(ZendeskConstants.Misc.ISO_INSTANT,
            ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT, ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT);

    public static long isoToEpochSecond(final String time)
    {
        Optional<String> pattern = supportedTimeFormat(time, supportedFormats);
        if (pattern.isPresent()) {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern.get());
            final OffsetDateTime offsetDateTime = LocalDateTime.parse(time, formatter).atOffset(ZoneOffset.UTC);
            return offsetDateTime.toInstant().getEpochSecond();
        }

        throw new DataException("Fail to parse value '" + time + "' follow formats " + "[ " + Joiner.on(",").join(supportedFormats) + "]");
    }

    private static Optional<String> supportedTimeFormat(final String value, final List<String> supportedFormats)
    {
        for (final String fmt : supportedFormats) {
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
}
