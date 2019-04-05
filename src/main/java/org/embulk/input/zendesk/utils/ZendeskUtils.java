package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import org.embulk.input.zendesk.models.Target;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

import java.util.Base64;
import java.util.function.Function;

public class ZendeskUtils
{
    private static final Logger logger = Exec.getLogger(ZendeskUtils.class);

    private ZendeskUtils()
    {}

    public static boolean isSupportIncremental(final Target target)
    {
        return !Target.TICKET_FORMS.equals(target)
                && !Target.TICKET_FIELDS.equals(target);
    }

    public static String convertBase64(final String text)
    {
        return Base64.getEncoder().encodeToString(text.getBytes(Charsets.UTF_8));
    }

    public static int numberToSplitWithHintingInTask(int count)
    {
        return (int) Math.ceil((double) count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE);
    }

    public static synchronized void addRecord(JsonNode record, Schema schema, PageBuilder pageBuilder)
    {
        schema.visitColumns(new ColumnVisitor() {
            @Override
            public void jsonColumn(Column column)
            {
                JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setJson(column, new JsonParser().parse(value.toString()));
                    return null;
                });
            }

            @Override
            public void stringColumn(Column column)
            {
                JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setString(column, value.asText());
                    return null;
                });
            }

            @Override
            public void timestampColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                setColumn(column, data, (value) -> {
                    Timestamp timestamp = getTimestampValue(value.asText());
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
            public void booleanColumn(Column column)
            {
                JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setBoolean(column, value.asBoolean());
                    return null;
                });
            }

            @Override
            public void longColumn(Column column)
            {
                JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setLong(column, value.asLong());
                    return null;
                });
            }

            @Override
            public void doubleColumn(Column column)
            {
                JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setDouble(column, value.asDouble());
                    return null;
                });
            }

            private void setColumn(Column column, JsonNode data, Function<JsonNode, Void> setter)
            {
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                    return;
                }
                setter.apply(data);
            }
        });
        pageBuilder.addRecord();
    }

    private static boolean isNull(JsonNode jsonNode)
    {
        return jsonNode == null || jsonNode.isNull();
    }

    /*
     * For getting the timestamp value of the node
     * Sometime if the parser could not parse the value then return null
     * */
    private static Timestamp getTimestampValue(String value)
    {
        Timestamp result = null;
        try {
            long timeStamp = ZendeskDateUtils.isoToEpochSecond(value);
            result = Timestamp.ofEpochSecond(timeStamp);
        }
        catch (Exception e) {
            logger.warn("Error when parse time stamp data " + value);
        }
        return result;
    }
}
