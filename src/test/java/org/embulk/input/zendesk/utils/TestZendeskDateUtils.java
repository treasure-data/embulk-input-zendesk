package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;
import org.junit.Assert;
import org.junit.Test;

public class TestZendeskDateUtils
{
    @Test
    public void isoToEpochSecondShouldReturnCorrectValue()
    {
        long expectedValue = 1550645445;

        long value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45Z");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20 06:50:45 +0000");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45.000Z");
        Assert.assertEquals(expectedValue, value);
    }

    @Test(expected = DataException.class)
    public void isoToEpochSecondShouldThrowException()
    {
        ZendeskDateUtils.isoToEpochSecond("2019-02asdasdasd-20T06:50:45Z");
    }
}
