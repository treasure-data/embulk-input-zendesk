package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.zendesk.utils.ZendeskTestHelper;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.msgpack.value.Value;

import static org.embulk.input.zendesk.ZendeskInputPlugin.CONFIG_MAPPER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.time.ZoneOffset;

public class TestRecordImporter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    public PageBuilder pageBuilder = mock(PageBuilder.class);

    private static Schema schema;
    private static Column booleanColumn;
    private static Column longColumn;
    private static Column doubleColumn;
    private static Column stringColumn;
    private static Column dateColumn;
    private static Column jsonColumn;
    private static ZendeskInputPlugin.PluginTask pluginTask;

    private RecordImporter recordImporter;

    @BeforeClass
    public static void setUp()
    {
        pluginTask = CONFIG_MAPPER.map(ZendeskTestHelper.getConfigSource("util.yml"), ZendeskInputPlugin.PluginTask.class);
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
        recordImporter = spy(new RecordImporter(schema, pageBuilder));
    }

    @Test
    public void testAddRecordAllRight()
    {
        String name = "allRight";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);

        Boolean boolValue = Boolean.TRUE;
        long longValue = 1;
        double doubleValue = 1;
        String stringValue = "string";
        Instant dateValue = TimestampFormatter.builder("%Y-%m-%dT%H:%M:%S%z", true).setDefaultZoneOffset(ZoneOffset.UTC).build()
            .parse("2019-01-01T00:00:00Z");
        Value jsonValue = new JsonParser().parse("{}");

        recordImporter.addRecord(dataJson);

        verify(pageBuilder, times(1)).setBoolean(booleanColumn, boolValue);
        verify(pageBuilder, times(1)).setLong(longColumn, longValue);
        verify(pageBuilder, times(1)).setDouble(doubleColumn, doubleValue);
        verify(pageBuilder, times(1)).setString(stringColumn, stringValue);
        verify(pageBuilder, times(1)).setTimestamp(dateColumn, Timestamp.ofInstant(dateValue));
        verify(pageBuilder, times(1)).setJson(jsonColumn, jsonValue);
    }

    @Test
    public void testAddRecordAllWrong()
    {
        String name = "allWrong";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);

        Value jsonValue = new JsonParser().parse("{}");

        recordImporter.addRecord(dataJson);

        verify(pageBuilder, times(1)).setNull(booleanColumn);
        verify(pageBuilder, times(1)).setNull(longColumn);
        verify(pageBuilder, times(1)).setNull(doubleColumn);
        verify(pageBuilder, times(1)).setNull(stringColumn);
        verify(pageBuilder, times(1)).setNull(dateColumn);
        verify(pageBuilder, times(1)).setJson(jsonColumn, jsonValue);
    }

    @Test
    public void testAddRecordAllMissing()
    {
        String name = "allMissing";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);

        recordImporter.addRecord(dataJson);

        verify(pageBuilder, times(6)).setNull(Mockito.any());
    }
}
