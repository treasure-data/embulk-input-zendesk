package org.embulk.input.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.input.zendesk.services.ZendeskSupportAPiService;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.spi.Schema;

public class ZendeskServiceDataSplitter<T extends RestClientInputTaskBase> extends ServiceDataSplitter<T>
{
    ZendeskSupportAPiService zendeskSupportAPiService;

    public ZendeskServiceDataSplitter(ZendeskSupportAPiService zendeskSupportAPiService)
    {
        this.zendeskSupportAPiService = zendeskSupportAPiService;
    }

    @Override
    public int numberToSplitWithHintingInTask(T taskToHint)
    {
        ZendeskInputPluginDelegate.PluginTask task = (ZendeskInputPluginDelegate.PluginTask) taskToHint;

        if (task.getIncremental()) {
            return 1;
        }

        if (task.getPage() > 0) {
            return task.getPage();
        }
        else {
            int numberOfPage = calculateNumberOfPages(task);
            if (numberOfPage > 0) {
                task.setPage(numberOfPage);
                return numberOfPage;
            }
        }

        return 1;
    }

    @Override
    public void hintInEachSplitTask(T taskToHint, Schema schema, int taskIndex)
    {
    }

    // only apply for non incremental
    private int calculateNumberOfPages(final ZendeskInputPluginDelegate.PluginTask task)
    {
        final JsonNode result = zendeskSupportAPiService.getData("", 0, false);

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
