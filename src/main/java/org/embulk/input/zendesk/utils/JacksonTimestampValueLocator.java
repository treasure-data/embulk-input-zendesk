package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.JacksonValueLocator;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class JacksonTimestampValueLocator extends JacksonValueLocator
{
    private final String name;

    public JacksonTimestampValueLocator(final String name)
    {
        this.name = name;
    }

    @Override
    public JsonNode seekValue(final ObjectNode record)
    {
        if (!record.get(name).asText().equals("null")) {
            final OffsetDateTime dateTime = OffsetDateTime.parse(record.get(name).asText());
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT)
                    .withZone(ZoneOffset.UTC);
            record.put(name, dateTimeFormatter.format(dateTime.toInstant()));
            return record.get(name);
        }
        else {
            return null;
        }
    }

    @Override
    public void placeValue(final ObjectNode record, final JsonNode value)
    {
        record.set(name, value);
    }
}
