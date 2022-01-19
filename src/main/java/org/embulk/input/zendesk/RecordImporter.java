package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.json.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class RecordImporter
{
    private Schema schema;
    private PageBuilder pageBuilder;

    private static final Logger logger = LoggerFactory.getLogger(RecordImporter.class);

    public RecordImporter(Schema schema, PageBuilder pageBuilder)
    {
        this.schema = schema;
        this.pageBuilder = pageBuilder;
    }

    public synchronized void addRecord(final JsonNode record)
    {
        schema.visitColumns(new ColumnVisitor()
        {
            @Override
            public void jsonColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setJson(column, new JsonParser().parse(value.toString()));
                    return null;
                });
            }

            @Override
            public void stringColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setString(column, value.asText());
                    return null;
                });
            }

            @Override
            public void timestampColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());
                setColumn(column, data, (value) -> {
                    final Timestamp timestamp = getTimestampValue(value.asText());
                    if (timestamp == null) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setTimestamp(column, timestamp);
                    }
                    return null;
                });
            }

            @Override
            public void booleanColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setBoolean(column, value.asBoolean());
                    return null;
                });
            }

            @Override
            public void longColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setLong(column, value.asLong());
                    return null;
                });
            }

            @Override
            public void doubleColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setDouble(column, value.asDouble());
                    return null;
                });
            }

            private void setColumn(final Column column, final JsonNode data, final Function<JsonNode, Void> setter)
            {
                if (ZendeskUtils.isNull(data)) {
                    pageBuilder.setNull(column);
                    return;
                }
                setter.apply(data);
            }
        });

        pageBuilder.addRecord();
    }

    /*
     * For getting the timestamp value of the node
     * Sometime if the parser could not parse the value then return null
     * */
    private Timestamp getTimestampValue(final String value)
    {
        Timestamp result = null;
        try {
            final long timeStamp = ZendeskDateUtils.isoToEpochSecond(value);
            result = Timestamp.ofEpochSecond(timeStamp);
        }
        catch (final Exception e) {
            logger.warn("Error when parse time stamp data " + value);
        }
        return result;
    }
}
