package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestZendeskDateUtils
{
    @Test
    public void testIsoToEpochSecondShouldReturnCorrectValue()
    {
        long expectedValue = 1550645445;

        long value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45Z");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20 06:50:45 +0000");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45.000Z");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45+00:00");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20 06:50:45+0000");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45.215149154Z");
        Assert.assertEquals(expectedValue, value);
    }

    @Test
    public void testIsoToEpochSecondShouldThrowException()
    {
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02asdasdasd-20T06:50:45Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-002-20T06:50:45Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02-200T06:50:45.000Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02-20T24:01:00Z"));
    }

    @Test
    public void testConvertToDateTimeFormat()
    {
        String expectedString = "2019-05-01T07:14:50Z";

        String actualString = ZendeskDateUtils.convertToDateTimeFormat("2019-05-01 07:14:50 +0000", ZendeskConstants.Misc.ISO_INSTANT);
        assertEquals(expectedString, actualString);

        actualString = ZendeskDateUtils.convertToDateTimeFormat("2019-05-01 07:14:50+0000", ZendeskConstants.Misc.ISO_INSTANT);
        assertEquals(expectedString, actualString);

        actualString = ZendeskDateUtils.convertToDateTimeFormat("2019-05-01T07:14:50.000Z", ZendeskConstants.Misc.ISO_INSTANT);
        assertEquals(expectedString, actualString);

        actualString = ZendeskDateUtils.convertToDateTimeFormat("2019-05-01T07:14:50+00:00", ZendeskConstants.Misc.ISO_INSTANT);
        assertEquals(expectedString, actualString);
    }

    @Test
    public void testSupportedTimeFormat()
    {
        Optional optional = ZendeskDateUtils.supportedTimeFormat("2019-05-01 07:14:50+0000");
        assertTrue(optional.isPresent());

        optional = ZendeskDateUtils.supportedTimeFormat("2019-05-01T07:14:50");
        assertFalse(optional.isPresent());
    }

    @Test
    public void testGetStartTime()
    {
        long expectedValue = 1550645445;
        long actualValue = ZendeskDateUtils.getStartTime("2019-02-20 06:512310:45 +0000");
        assertEquals(0, actualValue);

        actualValue = ZendeskDateUtils.getStartTime("2019-02-30 06:50:45 +0000");
        assertEquals(0, actualValue);

        actualValue = ZendeskDateUtils.getStartTime("2019-02-20 06:50:45 +0000");
        assertEquals(expectedValue, actualValue);
    }
}
