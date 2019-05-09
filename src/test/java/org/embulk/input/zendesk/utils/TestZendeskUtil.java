package org.embulk.input.zendesk.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.zendesk.models.Target;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestZendeskUtil
{
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testIsSupportIncrementalShouldReturnTrue()
    {
        boolean result = ZendeskUtils.isSupportAPIIncremental(Target.TICKETS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.USERS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.ORGANIZATIONS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.TICKET_EVENTS);
        Assert.assertTrue(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.TICKET_METRICS);
        Assert.assertTrue(result);
    }

    @Test
    public void testIsSupportIncrementalShouldReturnFail()
    {
        boolean result = ZendeskUtils.isSupportAPIIncremental(Target.TICKET_FORMS);
        Assert.assertFalse(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.TICKET_FIELDS);
        Assert.assertFalse(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.OBJECT_RECORDS);
        Assert.assertFalse(result);

        result = ZendeskUtils.isSupportAPIIncremental(Target.RELATIONSHIP_RECORDS);
        Assert.assertFalse(result);
    }

    @Test
    public void testConvertBase64()
    {
        String expectedResult = "YWhrc2RqZmhramFzZGhma2phaGRma2phaGRramZoYWtqZGY=";
        String encode = ZendeskUtils.convertBase64("ahksdjfhkjasdhfkjahdfkjahdkjfhakjdf");
        assertEquals(expectedResult, encode);
    }
}
