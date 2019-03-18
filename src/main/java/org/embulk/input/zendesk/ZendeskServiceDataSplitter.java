package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.input.zendesk.services.ZendeskSupportAPIService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.spi.Schema;

public class ZendeskServiceDataSplitter<T extends RestClientInputTaskBase> extends ServiceDataSplitter<T>
{
    ZendeskSupportAPIService zendeskSupportAPIService;

    public ZendeskServiceDataSplitter(final ZendeskSupportAPIService zendeskSupportAPiService)
    {
        this.zendeskSupportAPIService = zendeskSupportAPiService;
    }

    @Override
    public int numberToSplitWithHintingInTask(final T taskToHint)
    {
        final ZendeskInputPluginDelegate.PluginTask task = (ZendeskInputPluginDelegate.PluginTask) taskToHint;
        if (task.getIncremental()) {
            return 1;
        }
        return Math.max(calculateNumberOfPages(), 1);
    }

    @Override
    public void hintInEachSplitTask(final T taskToHint, final Schema schema, final int taskIndex)
    {
    }

    // only apply for non incremental
    private int calculateNumberOfPages()
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
}
