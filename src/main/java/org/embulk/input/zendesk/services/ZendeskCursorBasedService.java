package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Iterator;

import static org.embulk.input.zendesk.ZendeskInputPlugin.CONFIG_MAPPER_FACTORY;

public class ZendeskCursorBasedService
    implements ZendeskService
{
    private static final Logger logger = LoggerFactory.getLogger(ZendeskNormalServices.class);

    protected ZendeskInputPlugin.PluginTask task;

    private ZendeskRestClient zendeskRestClient;

    public ZendeskCursorBasedService(final ZendeskInputPlugin.PluginTask task)
    {
        this.task = task;
    }

    @Override
    public boolean isSupportIncremental()
    {
        return true;
    }

    @Override
    public TaskReport addRecordToImporter(int taskIndex, RecordImporter recordImporter)
    {
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        importData(task, recordImporter, taskReport);

        return taskReport;
    }

    @Override
    public JsonNode getDataFromPath(String path, int page, boolean isPreview, long startTime)
    {
        try {
            String buildPath = buildPath(0);
            final String response = getZendeskRestClient().doGet(buildPath, task, Exec.isPreview());
            return ZendeskUtils.parseJsonObject(response);
        }
        catch (URISyntaxException e) {
            throw new ConfigException(e);
        }
    }

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            zendeskRestClient = new ZendeskRestClient();
        }
        return zendeskRestClient;
    }

    private void importData(final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter, final TaskReport taskReport)
    {
        long initStartTime = 0;

        if (task.getStartTime().isPresent()) {
            initStartTime = ZendeskDateUtils.getStartTime(task.getStartTime().get());
        }

        long nextStartTime = initStartTime;
        long totalRecords = 0;
        try {
            String path = buildPath(initStartTime);

            while (true) {
                final JsonNode result = fetchResultFromPath(path);

                final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(result, task.getTarget().getJsonName());

                int numberOfRecords = 0;

                while (iterator.hasNext()) {
                    final JsonNode recordJsonNode = iterator.next();
                    fetchSubResourceAndAddToImporter(recordJsonNode, task, recordImporter);
                    numberOfRecords++;
                    // Store nextStartTime of last item
                    if (!iterator.hasNext() && task.getIncremental()) {
                        nextStartTime = ZendeskDateUtils.isoToEpochSecond(recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText());
                    }
                }

                totalRecords = totalRecords + numberOfRecords;
                if (result.has(ZendeskConstants.Field.END_OF_STREAM)) {
                    if (result.get(ZendeskConstants.Field.END_OF_STREAM).asBoolean()) {
                        break;
                    }
                }
                else {
                    throw new DataException("Missing end of stream, please double-check the endpoint");
                }
                if (Exec.isPreview()) {
                    break;
                }

                path = result.get(ZendeskConstants.Field.AFTER_URL).asText();
            }

            logger.info("import records total " + totalRecords);

            if (!Exec.isPreview() && task.getIncremental()) {
                storeStartTimeForConfigDiff(taskReport, nextStartTime);
            }
        }
        catch (Exception e) {
            throw new DataException(e);
        }
    }

    private String buildPath(long startTime)
        throws URISyntaxException
    {
        return ZendeskUtils.getURIBuilder(task.getLoginUrl()).setPath(ZendeskConstants.Url.API + "/" + "incremental" + "/" + task.getTarget().toString() + "/" + "cursor.json").build().toString() + "?start_time=" + startTime;
    }

    private JsonNode fetchResultFromPath(String path)
    {
        final String response = getZendeskRestClient().doGet(path, task, Exec.isPreview());
        return ZendeskUtils.parseJsonObject(response);
    }

    private void fetchSubResourceAndAddToImporter(final JsonNode jsonNode, final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter)
    {
        task.getIncludes().forEach(include -> {
            final String relatedObjectName = include.trim();

            final URIBuilder uriBuilder = ZendeskUtils.getURIBuilder(task.getLoginUrl()).setPath(ZendeskConstants.Url.API + "/" + task.getTarget().toString() + "/" + jsonNode.get(ZendeskConstants.Field.ID).asText() + "/" + relatedObjectName + ".json");
            try {
                final JsonNode result = getDataFromPath(uriBuilder.toString(), 0, false, 0);
                if (result != null && result.has(relatedObjectName)) {
                    ((ObjectNode) jsonNode).set(include, result.get(relatedObjectName));
                }
            }
            catch (final ConfigException e) {
                // Sometimes we get 404 when having invalid endpoint, so ignore when we get 404 InvalidEndpoint
                if (!(e.getCause() instanceof ZendeskException && ((ZendeskException) e.getCause()).getStatusCode() == HttpStatus.SC_NOT_FOUND)) {
                    throw e;
                }
            }
        });

        recordImporter.addRecord(jsonNode);
    }

    private void storeStartTimeForConfigDiff(final TaskReport taskReport, final long nextStartTime)
    {
        taskReport.set(ZendeskConstants.Field.START_TIME, nextStartTime);
    }
}
