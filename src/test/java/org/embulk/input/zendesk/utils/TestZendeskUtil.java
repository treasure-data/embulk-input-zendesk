package org.embulk.input.zendesk.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestZendeskUtil
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testConvertBase64()
    {
        String expectedResult = "YWhrc2RqZmhramFzZGhma2phaGRma2phaGRramZoYWtqZGY=";
        String encode = ZendeskUtils.convertBase64("ahksdjfhkjasdhfkjahdfkjahdkjfhakjdf");
        assertEquals(expectedResult, encode);
    }
}
