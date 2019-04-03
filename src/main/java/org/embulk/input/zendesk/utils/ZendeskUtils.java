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
        return count % ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE == 0
                ? count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE
                : (count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE) + 1;
    }

    public static synchronized void addRecord(JsonNode record, Schema schema, PageBuilder pageBuilder)
    {
        schema.visitColumns(new ColumnVisitor() {
            @Override
            public void jsonColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    pageBuilder.setJson(column, new JsonParser().parse(data.toString()));
                }
            }

            @Override
            public void stringColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    pageBuilder.setString(column, data.asText());
                }
            }

            @Override
            public void timestampColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    Timestamp value = getTimestampValue(data.asText());
                    if (value == null) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setTimestamp(column, value);
                    }
                }
            }

            @Override
            public void booleanColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    Boolean value = getBooleanValue(data);
                    pageBuilder.setBoolean(column, value);
                }
            }

            @Override
            public void longColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    Long value = getLongValue(data);
                    pageBuilder.setLong(column, value);
                }
            }

            @Override
            public void doubleColumn(Column column)
            {
                JsonNode data = record.get(column.getName());
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                }
                else {
                    Double value = getDoubleValue(data);
                    pageBuilder.setDouble(column, value);
                }
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

    private static Long getLongValue(JsonNode value)
    {
        return value.asLong();
    }

    private static Boolean getBooleanValue(JsonNode value)
    {
        return value.asBoolean();
    }

    private static Double getDoubleValue(JsonNode value)
    {
        return value.asDouble();
    }
}
