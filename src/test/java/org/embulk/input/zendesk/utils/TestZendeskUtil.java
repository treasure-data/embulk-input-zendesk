package org.embulk.input.zendesk.utils;

import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestZendeskUtil
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testConvertBase64()
    {
        String expectedResult = "YWhrc2RqZmhramFzZGhma2phaGRma2phaGRramZoYWtqZGY=";
        String encode = ZendeskUtils.convertBase64("ahksdjfhkjasdhfkjahdfkjahdkjfhakjdf");
        assertEquals(expectedResult, encode);
    }
}
