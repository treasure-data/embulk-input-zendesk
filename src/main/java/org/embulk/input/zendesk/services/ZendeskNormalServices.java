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
import org.embulk.input.zendesk.models.Target;
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

    protected abstract String buildURI(int page, long startTime);

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        if (zendeskRestClient == null) {
            zendeskRestClient = new ZendeskRestClient();
        }
        return zendeskRestClient;
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
        try {
            final String response = getZendeskRestClient().doGet(path, task, isPreview);
            return ZendeskUtils.parseJsonObject(response);
        }
        catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }

    protected URIBuilder getURIBuilderFromHost()
    {
        return ZendeskUtils.getURIBuilder(task.getLoginUrl());
    }

    private void importDataForIncremental(final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter, final TaskReport taskReport)
    {
        long startTime = 0;
        long endTime = Long.MAX_VALUE;
        long lastTime = 0;

        if (!Exec.isPreview() && isSupportIncremental()) {
            if (task.getStartTime().isPresent()) {
                startTime = ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get());
            }
            if (task.getEndTime().isPresent()) {
                endTime = ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get());
            }
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
                final long resultEndTime = result.get(ZendeskConstants.Field.END_TIME).asLong();

                int numberOfRecords = 0;
                if (result.has(ZendeskConstants.Field.COUNT)) {
                    numberOfRecords = result.get(ZendeskConstants.Field.COUNT).asInt();
                }

                while (iterator.hasNext()) {
                    final JsonNode recordJsonNode = iterator.next();

                    if (isUpdatedBySystem(recordJsonNode, startTime)) {
                        continue;
                    }

                    // Contain some records  that later than end_time. Checked and don't add.
                    // Because the api already sorted by updated_at or timestamp for ticket_events, we just need to break no need to check further.
                    if (resultEndTime > endTime) {
                        long checkedTime = 0;
                        if (recordJsonNode.has(ZendeskConstants.Field.UPDATED_AT) && !recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).isNull()) {
                            checkedTime = ZendeskDateUtils.isoToEpochSecond(recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).textValue());
                        }

                        // ticket events is updated by system not user's action so it only has timestamp field
                        if (task.getTarget().equals(Target.TICKET_EVENTS) && recordJsonNode.has("timestamp") && !recordJsonNode.get("timestamp").isNull()) {
                            checkedTime = recordJsonNode.get("timestamp").asLong();
                        }

                        // scores (or response) is only store rated_at time
                        if (task.getTarget().equals(Target.SCORES) && recordJsonNode.has("rated_at") && !recordJsonNode.get("rated_at").isNull()) {
                            checkedTime = ZendeskDateUtils.isoToEpochSecond(recordJsonNode.get("rated_at").textValue());
                        }

                        if (checkedTime > endTime) {
                            break;
                        }
                        else {
                            lastTime = Math.max(checkedTime, lastTime);
                        }
                    }

                    if (task.getDedup()) {
                        final String recordID = recordJsonNode.get(ZendeskConstants.Field.ID).asText();

                        // add success -> no duplicate
                        if (!knownIds.add(recordID)) {
                            continue;
                        }
                    }

                    pool.submit(() -> fetchData(recordJsonNode, task, recordImporter));
                    recordCount++;
                    if (Exec.isPreview()) {
                        return;
                    }
                }

                ZendeskNormalServices.logger.info("Fetched '{}' records from start_time '{}'", recordCount, startTime);

                // store the last end_time for next run
                if (task.getIncremental() && !Exec.isPreview()) {
                    lastTime = Math.max(resultEndTime, lastTime);
                }

                if (numberOfRecords < ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL || resultEndTime >= endTime) {
                    break;
                }

                startTime = resultEndTime;
            }

            if (!Exec.isPreview()) {
                storeStartTimeForConfigDiff(taskReport, lastTime);
            }
        }
        finally {
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
                catch (final InterruptedException e) {
                    ZendeskNormalServices.logger.warn("Error when wait pool to finish");
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private void storeStartTimeForConfigDiff(final TaskReport taskReport, final long lastTime)
    {
        if (task.getIncremental()) {
            // no records
            long nextStartTime = lastTime + 1;
            if (lastTime == 0) {
                nextStartTime = Instant.now().getEpochSecond();
            }
            else {
                // have end_time -> store min of (end_time + 1, last_time + 1)
                // end_time can be set in the future
                if (task.getEndTime().isPresent()) {
                    nextStartTime = Math.min(ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get()) + 1,  lastTime + 1);
                }
                else {
                    // don't have end_time -> store last_time + 1
                    nextStartTime = lastTime + 1;
                }
            }
            taskReport.set(ZendeskConstants.Field.START_TIME, nextStartTime);

            if (task.getEndTime().isPresent()) {
                long endTime = ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get());
                long startTime = ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get());
                taskReport.set(ZendeskConstants.Field.END_TIME, nextStartTime + startTime - endTime);
            }
        }
    }

    private void fetchData(final JsonNode jsonNode, final ZendeskInputPlugin.PluginTask task, final RecordImporter recordImporter)
    {
        task.getIncludes().forEach(include -> {
            final String relatedObjectName = include.trim();

            final URIBuilder uriBuilder = getURIBuilderFromHost()
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
            fetchData(iterator.next(), task, recordImporter);

            if (Exec.isPreview()) {
                break;
            }
        }
    }
}