package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;

import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.msgpack.value.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestZendeskBaseService
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static Schema schema;
    private static Column booleanColumn;
    private static Column longColumn;
    private static Column doubleColumn;
    private static Column stringColumn;
    private static Column dateColumn;
    private static Column jsonColumn;
    private static PluginTask pluginTask;

    private ZendeskBaseServices zendeskBaseServices;

    @BeforeClass
    public static void setUp()
    {
        pluginTask = ZendeskTestHelper.getConfigSource("util.yml").loadConfig(PluginTask.class);
        schema = pluginTask.getColumns().toSchema();
        booleanColumn = schema.getColumn(0);
        longColumn = schema.getColumn(1);
        doubleColumn = schema.getColumn(2);
        stringColumn = schema.getColumn(3);
        dateColumn = schema.getColumn(4);
        jsonColumn = schema.getColumn(5);
    }

    @Before
    public void prepare()
    {
        zendeskBaseServices = spy(new ZendeskSupportAPIService(pluginTask));
    }

    @Test
    public void testAddRecordAllRight()
    {
        String name = "allRight";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);
        PageBuilder mock = mock(PageBuilder.class);

        Boolean boolValue = Boolean.TRUE;
        long longValue = 1;
        double doubleValue = 1;
        String stringValue = "string";
        Timestamp dateValue = TimestampParser.of("%Y-%m-%dT%H:%M:%S%z", "UTC").parse("2019-01-01T00:00:00Z");
        Value jsonValue = new JsonParser().parse("{}");

        zendeskBaseServices.addRecord(dataJson, schema, mock);

        verify(mock, times(1)).setBoolean(booleanColumn, boolValue);
        verify(mock, times(1)).setLong(longColumn, longValue);
        verify(mock, times(1)).setDouble(doubleColumn, doubleValue);
        verify(mock, times(1)).setString(stringColumn, stringValue);
        verify(mock, times(1)).setTimestamp(dateColumn, dateValue);
        verify(mock, times(1)).setJson(jsonColumn, jsonValue);
    }

    @Test
    public void testAddRecordAllWrong()
    {
        String name = "allWrong";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);
        PageBuilder mock = mock(PageBuilder.class);

        Value jsonValue = new JsonParser().parse("{}");

        zendeskBaseServices.addRecord(dataJson, schema, mock);

        verify(mock, times(1)).setNull(booleanColumn);
        verify(mock, times(1)).setNull(longColumn);
        verify(mock, times(1)).setNull(doubleColumn);
        verify(mock, times(1)).setNull(stringColumn);
        verify(mock, times(1)).setNull(dateColumn);
        verify(mock, times(1)).setJson(jsonColumn, jsonValue);
    }

    @Test
    public void testAddRecordAllMissing()
    {
        String name = "allMissing";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);
        PageBuilder mock = mock(PageBuilder.class);

        zendeskBaseServices.addRecord(dataJson, schema, mock);

        verify(mock, times(6)).setNull(Mockito.any());
    }
}
