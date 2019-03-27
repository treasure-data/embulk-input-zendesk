package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.msgpack.value.Value;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZendeskUtil
{
    private static Schema schema;
    private static Column booleanColumn;
    private static Column longColumn;
    private static Column doubleColumn;
    private static Column stringColumn;
    private static Column dateColumn;
    private static Column jsonColumn;

    private ZendeskSupportAPIService zendeskSupportAPIService = mock(ZendeskSupportAPIService.class);

    @BeforeClass
    public static void setUp()
    {
        PluginTask pluginTask = ZendeskTestHelper.getConfigSource("util.yml").loadConfig(PluginTask.class);
        schema = pluginTask.getColumns().toSchema();
        booleanColumn = schema.getColumn(0);
        longColumn = schema.getColumn(1);
        doubleColumn = schema.getColumn(2);
        stringColumn = schema.getColumn(3);
        dateColumn = schema.getColumn(4);
        jsonColumn = schema.getColumn(5);
    }

    @Test
    public void testIsSupportIncrementalShouldReturnTrue()
    {
        boolean result = ZendeskUtils.isSupportIncremental(Target.TICKETS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportIncremental(Target.USERS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportIncremental(Target.ORGANIZATIONS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportIncremental(Target.TICKET_EVENTS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportIncremental(Target.TICKET_METRICS);
        Assert.assertTrue(result);
    }

    @Test
    public void testIsSupportIncrementalShouldReturnFalse()
    {
        boolean result = ZendeskUtils.isSupportInclude(Target.TICKET_FIELDS);
        Assert.assertFalse(result);

        result = ZendeskUtils.isSupportInclude(Target.TICKET_FORMS);
        Assert.assertFalse(result);
    }

    @Test
    public void testIsSupportIncludeShouldReturnTrue()
    {
        boolean result = ZendeskUtils.isSupportInclude(Target.TICKETS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportInclude(Target.USERS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportInclude(Target.ORGANIZATIONS);
        Assert.assertTrue(result);
    }

    @Test
    public void testIsSupportIncludeWithIncludeListShouldReturnTrue()
    {
        boolean result = ZendeskUtils.isSupportInclude(Target.TICKETS, Collections.singletonList("abc"));
        Assert.assertTrue(result);
    }

    @Test
    public void testIsSupportIncludeWithIncludeListShouldReturnFalse()
    {
        boolean result = ZendeskUtils.isSupportInclude(Target.TICKETS, Collections.EMPTY_LIST);
        Assert.assertFalse(result);
    }

    @Test
    public void testNumberToSplitWithHintingInTaskWithIncrementalTarget()
    {
        PluginTask task = ZendeskTestHelper.getConfigSource("incremental.yml").loadConfig(PluginTask.class);
        int number = ZendeskUtils.numberToSplitWithHintingInTask(task, zendeskSupportAPIService);
        assertEquals(1, number);
    }

    @Test
    public void testNumberToSplitWithHintingInTaskWithNonIncrementalTarget()
    {
        PluginTask task = ZendeskTestHelper.getConfigSource("non-incremental.yml").loadConfig(PluginTask.class);
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util_page.json");
        when(zendeskSupportAPIService.getData(anyString(), anyInt(), anyBoolean())).thenReturn(dataJson);
        int number = ZendeskUtils.numberToSplitWithHintingInTask(task, zendeskSupportAPIService);
        assertEquals(22, number);
    }

    @Test
    public void testAddRecordAllRight()
    {
        String name = "allRight";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);
        PageBuilder mock = Mockito.mock(PageBuilder.class);

        Boolean boolValue = Boolean.TRUE;
        long longValue = 1;
        long doubleValue = 1;
        String stringValue = "string";
        Timestamp dateValue = TimestampParser.of("%Y-%m-%dT%H:%M:%S.%L%z", "UTC").parse("2019-01-01T00:00:00.000Z");
        Value jsonValue = new JsonParser().parse("{}");

        ZendeskUtils.addRecord(dataJson, schema, mock);

        verify(mock, times(1)).setBoolean(booleanColumn, boolValue);
        verify(mock, times(1)).setLong(longColumn, longValue);
        verify(mock, times(1)).setLong(doubleColumn, doubleValue);
        verify(mock, times(1)).setString(stringColumn, stringValue);
        verify(mock, times(1)).setTimestamp(dateColumn, dateValue);
        verify(mock, times(1)).setJson(jsonColumn, jsonValue);
    }

    @Test
    public void testAddRecordAllWrong()
    {
        String name = "allWrong";
        JsonNode dataJson = ZendeskTestHelper.getJsonFromFile("data/util.json").get(name);
        PageBuilder mock = Mockito.mock(PageBuilder.class);

        Value jsonValue = new JsonParser().parse("{}");

        ZendeskUtils.addRecord(dataJson, schema, mock);

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
        PageBuilder mock = Mockito.mock(PageBuilder.class);

        ZendeskUtils.addRecord(dataJson, schema, mock);

        verify(mock, times(6)).setNull(Mockito.any(Column.class));
    }

    @Test
    public void testConvertBase64()
    {
        String expectedResult = "YWhrc2RqZmhramFzZGhma2phaGRma2phaGRramZoYWtqZGY=";
        String encode = ZendeskUtils.convertBase64("ahksdjfhkjasdhfkjahdfkjahdkjfhakjdf");
        assertEquals(expectedResult, encode);
    }
}
