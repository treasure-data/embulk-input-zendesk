package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;

import java.util.Base64;
import java.util.List;

public class ZendeskUtils
{
    private ZendeskUtils()
    {}

    public static boolean isSupportIncremental(final Target target)
    {
        return !Target.TICKET_FORMS.equals(target) && !Target.TICKET_FIELDS.equals(target);
    }

    // For more information: https://developer.zendesk.com/rest_api/docs/support/side_loading#supported-endpoints
    public static boolean isSupportInclude(final Target target)
    {
        return Target.TICKETS.equals(target)
                || Target.USERS.equals(target)
                || Target.ORGANIZATIONS.equals(target);
    }

    public static String convertBase64(final String text)
    {
        return Base64.getEncoder().encodeToString(text.getBytes(Charsets.UTF_8));
    }

    public static boolean isSupportInclude(Target target, List<String> includes)
    {
        return includes != null && !includes.isEmpty() && ZendeskUtils.isSupportInclude(target);
    }

    public static int numberToSplitWithHintingInTask(final ZendeskInputPlugin.PluginTask task,
                                                     final ZendeskSupportAPIService zendeskSupportAPIService)
    {
        if (isSupportIncremental(task.getTarget())) {
            return 1;
        }
        return Math.max(calculateNumberOfPages(zendeskSupportAPIService), 1);
    }

    // only apply for non incremental
    private static int calculateNumberOfPages(final ZendeskSupportAPIService zendeskSupportAPIService)
    {
        final JsonNode result = zendeskSupportAPIService.getData("", 0, false);

        if (result.get(ZendeskConstants.Field.COUNT) != null
                && result.get(ZendeskConstants.Field.COUNT).isInt()) {
            int count = result.get(ZendeskConstants.Field.COUNT).asInt();

            return count % ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE == 0
                    ? count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE
                    : (count / ZendeskConstants.Misc.RECORDS_SIZE_PER_PAGE) + 1;
        }
        return 1;
    }

    public static void addRecord(JsonNode record, Schema schema, PageBuilder pageBuilder)
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
                longColumn(column);
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
        }
        return result;
    }

    /*
     * For getting the Long value of the node
     * Sometime if error occurs (i.e a JSON value but user modified it as long) then return null
     * */
    private static Long getLongValue(JsonNode value)
    {
        Long result = null;
        try {
            result = value.asLong();
        }
        catch (Exception e) {
        }
        return result;
    }

    /*
     * For getting the Boolean value of the node
     * Sometime if error occurs (i.e a JSON value but user modified it as boolean) then return null
     * */
    private static Boolean getBooleanValue(JsonNode value)
    {
        Boolean result = null;
        try {
            result = value.asBoolean();
        }
        catch (Exception e) {
        }
        return result;
    }
}
