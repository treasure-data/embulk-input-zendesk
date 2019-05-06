package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.ZendeskInputPlugin.PluginTask;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

import java.util.Iterator;

public class ZendeskSupportAPIService extends ZendeskBaseServices implements ZendeskService
{
    public ZendeskSupportAPIService(final PluginTask task)
    {
        super(task);
    }

    @Override
    public JsonNode getData(final String path, final int page, final boolean isPreview, final long startTime)
    {
        return super.getData(path, page, isPreview, startTime);
    }

    @Override
    public TaskReport execute(final int taskIndex, final Schema schema, final PageBuilder pageBuilder)
    {
        TaskReport taskReport = Exec.newTaskReport();

        if (ZendeskUtils.isSupportAPIIncremental(task.getTarget())) {
            importDataForIncremental(task, schema, pageBuilder, taskReport);
        }
        else {
            importDataForNonIncremental(task, taskIndex, schema, pageBuilder);
        }

        return taskReport;
    }

    @Override
    protected String buildURI(final int page, long startTime)
    {
        final boolean isSupportIncremental = ZendeskUtils.isSupportAPIIncremental(task.getTarget());

        final URIBuilder uriBuilder = getURIBuilderFromHost().setPath(buildPath(isSupportIncremental));

        if (isSupportIncremental) {
            uriBuilder.setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime));
            if (Target.TICKET_METRICS.equals(task.getTarget())) {
                uriBuilder.setParameter("include", "metric_sets");
            }
        }
        else {
            uriBuilder.setParameter("sort_by", "id")
                    .setParameter("per_page", String.valueOf(100))
                    .setParameter("page", String.valueOf(page));
        }

        return uriBuilder.toString();
    }

    private String buildPath(final boolean isSupportIncremental)
    {
        return (isSupportIncremental
                ? ZendeskConstants.Url.API_INCREMENTAL
                : ZendeskConstants.Url.API) +
                "/" +
                (Target.TICKET_METRICS.equals(task.getTarget())
                        ? Target.TICKETS.toString()
                        : task.getTarget().toString())
                + ".json";
    }

    private void importDataForNonIncremental(final ZendeskInputPlugin.PluginTask task, final int taskIndex, final Schema schema,
            final PageBuilder pageBuilder)
    {
        // Page start from 1 => page = taskIndex + 1
        final JsonNode result = getData("", taskIndex + 1, false, 0);
        final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(result, task.getTarget().getJsonName());

        while (iterator.hasNext()) {
            fetchData(iterator.next(), task, schema, pageBuilder);

            if (Exec.isPreview()) {
                break;
            }
        }
    }
}
