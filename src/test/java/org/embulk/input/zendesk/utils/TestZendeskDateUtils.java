package org.embulk.input.zendesk.utils;

import org.embulk.spi.DataException;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertThrows;

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

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20T06:50:45+00:00");
        Assert.assertEquals(expectedValue, value);

        value = ZendeskDateUtils.isoToEpochSecond("2019-02-20 06:50:45+0000");
        Assert.assertEquals(expectedValue, value);
    }

    @Test
    public void isoToEpochSecondShouldThrowException()
    {
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02asdasdasd-20T06:50:45Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-002-20T06:50:45Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02-200T06:50:45.000Z"));
        assertThrows(DataException.class, () -> ZendeskDateUtils.isoToEpochSecond("2019-02-20T24:01:00Z"));
    }
}
