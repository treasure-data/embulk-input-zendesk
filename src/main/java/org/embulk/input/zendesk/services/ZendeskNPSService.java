package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

public class ZendeskNPSService extends ZendeskBaseServices implements ZendeskService
{
    public ZendeskNPSService(final ZendeskInputPlugin.PluginTask task)
    {
        super(task);
    }

    @Override
    public TaskReport execute(final ZendeskInputPlugin.PluginTask task, final int taskIndex, final Schema schema, final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();
        importDataForIncremental(task, schema, pageBuilder, taskReport);
        return taskReport;
    }

    @Override
    public JsonNode getData(final String path, final int page, final boolean isPreview, final long startTime)
    {
        return super.getData(path, page, isPreview, startTime);
    }

    @Override
    protected String buildURI(final int page, final long startTime)
    {
        return getURIBuilderFromHost()
                .setPath(ZendeskConstants.Url.API_NPS_INCREMENTAL
                        + "/"
                        + task.getTarget().getJsonName()
                        + ".json")
                .setParameter(ZendeskConstants.Field.START_TIME, String.valueOf(startTime))
                .toString();
    }
}
