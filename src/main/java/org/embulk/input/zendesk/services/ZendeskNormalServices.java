package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
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
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.time.Instant;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ZendeskNormalServices implements ZendeskService
{
    private static final Logger logger = Exec.getLogger(ZendeskNormalServices.class);

    protected ZendeskInputPlugin.PluginTask task;

    private ZendeskRestClient zendeskRestClient;

    protected ZendeskNormalServices(final ZendeskInputPlugin.PluginTask task)
    {
        this.task = task;
    }

    public TaskReport execute(final int taskIndex, final RecordImporter recordImporter)
    {
        TaskReport taskReport = Exec.newTaskReport();

        if (isSupportIncremental()) {
            importDataForIncremental(task, recordImporter, taskReport);
        }
        else {
            importDataForNonIncremental(task, taskIndex, recordImporter);
        }

        return taskReport;
    }

    public JsonNode getData(String path, final int page, final boolean isPreview, final long startTime)
    {
        if (path.isEmpty()) {
            path = buildURI(page, startTime);
        }

        final String response = getZendeskRestClient().doGet(path, task, isPreview);
        return ZendeskUtils.parseJsonObject(response);
    }

    protected abstract String buildURI(int page, long startTime);

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            zendeskRestClient = new ZendeskRestClient();
        }
        return zendeskRestClient;
    }

    private void importDataForIncremental(final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter, final TaskReport taskReport)
    {
        long startTime = 0;

        if (task.getStartTime().isPresent()) {
            startTime = ZendeskDateUtils.getStartTime(task.getStartTime().get());
        }

        // For incremental target, we will run in one task but split in multiple threads inside for data deduplication.
        // Run with incremental will contain duplicated data.
        ThreadPoolExecutor pool = null;
        try {
            final Set<String> knownIds = ConcurrentHashMap.newKeySet();
            pool = new ThreadPoolExecutor(
                    10, 100, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
            );

            while (true) {
                int recordCount = 0;

                // Page argument isn't used in incremental API so we just set it to 0
                final JsonNode result = getData("", 0, false, startTime);
                final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(result, task.getTarget().getJsonName());

                int numberOfRecords = 0;
                if (result.has(ZendeskConstants.Field.COUNT)) {
                    numberOfRecords = result.get(ZendeskConstants.Field.COUNT).asInt();
                }

                while (iterator.hasNext()) {
                    final JsonNode recordJsonNode = iterator.next();

                    if (isUpdatedBySystem(recordJsonNode, startTime)) {
                        continue;
                    }

                    if (task.getDedup()) {
                        final String recordID = recordJsonNode.get(ZendeskConstants.Field.ID).asText();

                        // add success -> no duplicate
                        if (!knownIds.add(recordID)) {
                            continue;
                        }
                    }

                    pool.submit(() -> fetchRecord(recordJsonNode, task, recordImporter));
                    recordCount++;
                    if (Exec.isPreview()) {
                        return;
                    }
                }

                logger.info("Fetched '{}' records from start_time '{}'", recordCount, startTime);

                startTime = result.get(ZendeskConstants.Field.END_TIME).asLong();

                if (numberOfRecords < ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL) {
                    break;
                }
            }

            if (!Exec.isPreview()) {
                storeStartTimeForConfigDiff(taskReport, startTime);
            }
        }
        finally {
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
                catch (final InterruptedException e) {
                    logger.warn("Error when wait pool to finish");
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private void storeStartTimeForConfigDiff(final TaskReport taskReport, final long resultEndTime)
    {
        if (task.getIncremental()) {
            if (task.getEndTime().isPresent()) {
                taskReport.set(ZendeskConstants.Field.START_TIME, ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get()) + 1);
            }
            else {
                // resultEndTime = 0 mean no records, we should store now time for next run
                if (resultEndTime == 0) {
                    taskReport.set(ZendeskConstants.Field.START_TIME, Instant.now().getEpochSecond());
                }
                else {
                    // NOTE: start_time compared as "=>", not ">".
                    // If we will use end_time for next start_time, we got the same records that are fetched
                    // end_time + 1 is workaround for that
                    taskReport.set(ZendeskConstants.Field.START_TIME, resultEndTime + 1);
                }
            }
        }
    }

    private void fetchRecord(final JsonNode jsonNode, final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter)
    {
        task.getIncludes().forEach(include -> {
            final String relatedObjectName = include.trim();

            final URIBuilder uriBuilder = ZendeskUtils.getURIBuilder(task.getLoginUrl())
                    .setPath(ZendeskConstants.Url.API
                            + "/" + task.getTarget().toString()
                            + "/" + jsonNode.get(ZendeskConstants.Field.ID).asText()
                            + "/" + relatedObjectName + ".json");
            try {
                final JsonNode result = getData(uriBuilder.toString(), 0, false, 0);
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

    private boolean isUpdatedBySystem(final JsonNode recordJsonNode, final long startTime)
    {
        /*
         * https://developer.zendesk.com/rest_api/docs/core/incremental_export#excluding-system-updates
         * "generated_timestamp" will be updated when Zendesk internal changing
         * "updated_at" will be updated when ticket data was changed
         * start_time for query parameter will be processed on Zendesk with generated_timestamp,
         * but it was calculated by record' updated_at time.
         * So the doesn't changed record from previous import would be appear by Zendesk internal changes.
         */
        if (recordJsonNode.has(ZendeskConstants.Field.GENERATED_TIMESTAMP) && recordJsonNode.has(ZendeskConstants.Field.UPDATED_AT)) {
            final String recordUpdatedAtTime = recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText();
            final long recordUpdatedAtToEpochSecond = ZendeskDateUtils.isoToEpochSecond(recordUpdatedAtTime);

            return recordUpdatedAtToEpochSecond <= startTime;
        }

        return false;
    }

    private void importDataForNonIncremental(final ZendeskInputPlugin.PluginTask task, final int taskIndex, RecordImporter recordImporter)
    {
        // Page start from 1 => page = taskIndex + 1
        final JsonNode result = getData("", taskIndex + 1, false, 0);
        final Iterator<JsonNode> iterator = ZendeskUtils.getListRecords(result, task.getTarget().getJsonName());

        while (iterator.hasNext()) {
            fetchRecord(iterator.next(), task, recordImporter);

            if (Exec.isPreview()) {
                break;
            }
        }
    }
}
