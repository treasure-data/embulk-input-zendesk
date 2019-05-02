package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;
import org.embulk.input.zendesk.models.ZendeskException;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class ZendeskBaseServices
{
    protected static final Logger logger = Exec.getLogger(ZendeskBaseServices.class);

    protected ZendeskInputPlugin.PluginTask task;

    protected ZendeskBaseServices(final ZendeskInputPlugin.PluginTask task)
    {
        this.task = task;
    }

    protected abstract String buildURI(int page, long startTime);

    @VisibleForTesting
    protected ZendeskRestClient getZendeskRestClient()
    {
        return new ZendeskRestClient();
    }

    protected JsonNode getData(String path, final int page, final boolean isPreview, final long startTime)
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

    protected void importDataForIncremental(final ZendeskInputPlugin.PluginTask task, final Schema schema, final PageBuilder pageBuilder, final TaskReport taskReport)
    {
        long startTime = 0;

        if (!Exec.isPreview() && ZendeskUtils.isSupportAPIIncremental(task.getTarget()) && task.getStartTime().isPresent()) {
            startTime = ZendeskDateUtils.isoToEpochSecond(task.getStartTime().get());
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
                        if (knownIds.contains(recordID)) {
                            continue;
                        }
                        knownIds.add(recordID);
                    }

                    pool.submit(() -> fetchData(recordJsonNode, task, schema, pageBuilder));
                    recordCount++;
                    if (Exec.isPreview()) {
                        return;
                    }
                }

                ZendeskBaseServices.logger.info("Fetched '{}' records from start_time '{}'", recordCount, startTime);

                if (task.getIncremental() && !Exec.isPreview()) {
                    if (result.has(ZendeskConstants.Field.END_TIME) && !result.get(ZendeskConstants.Field.END_TIME).isNull()
                            && result.has(task.getTarget().getJsonName())) {
                        // NOTE: start_time compared as "=>", not ">".
                        // If we will use end_time for next start_time, we got the same record that is last fetched
                        // end_time + 1 is workaround for that
                        taskReport.set(ZendeskConstants.Field.START_TIME, result.get(ZendeskConstants.Field.END_TIME).asLong() + 1);
                    }
                    else {
                        // Sometimes no record and no end_time fetched on the job, but we should generate start_time on config_diff.
                        taskReport.set(ZendeskConstants.Field.START_TIME, Instant.now().getEpochSecond());
                    }
                }

                if (numberOfRecords < ZendeskConstants.Misc.MAXIMUM_RECORDS_INCREMENTAL) {
                    break;
                }
                else {
                    startTime = result.get(ZendeskConstants.Field.END_TIME).asLong();
                }
            }
        }

        finally {
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
                catch (final InterruptedException e) {
                    ZendeskBaseServices.logger.warn("Error when wait pool to finish");
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    protected URIBuilder getURIBuilderFromHost()
    {
        final URI uri;
        try {
            uri = new URI(task.getLoginUrl());
        }
        catch (final URISyntaxException e) {
            throw new ConfigException("Login url is invalid format " + e.getMessage());
        }

        return new URIBuilder()
                .setScheme(uri.getScheme())
                .setHost(uri.getHost());
    }

    protected void fetchData(final JsonNode jsonNode, final ZendeskInputPlugin.PluginTask task, final Schema schema,
            final PageBuilder pageBuilder)
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

        addRecord(jsonNode, schema, pageBuilder);
    }

    protected synchronized void addRecord(final JsonNode record, final Schema schema, final PageBuilder pageBuilder)
    {
        schema.visitColumns(new ColumnVisitor()
        {
            @Override
            public void jsonColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setJson(column, new JsonParser().parse(value.toString()));
                    return null;
                });
            }

            @Override
            public void stringColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setString(column, value.asText());
                    return null;
                });
            }

            @Override
            public void timestampColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());
                setColumn(column, data, (value) -> {
                    final Timestamp timestamp = getTimestampValue(value.asText());
                    if (timestamp == null) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setTimestamp(column, timestamp);
                    }
                    return null;
                });
            }

            @Override
            public void booleanColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setBoolean(column, value.asBoolean());
                    return null;
                });
            }

            @Override
            public void longColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setLong(column, value.asLong());
                    return null;
                });
            }

            @Override
            public void doubleColumn(final Column column)
            {
                final JsonNode data = record.get(column.getName());

                setColumn(column, data, (value) -> {
                    pageBuilder.setDouble(column, value.asDouble());
                    return null;
                });
            }

            private void setColumn(final Column column, final JsonNode data, final Function<JsonNode, Void> setter)
            {
                if (isNull(data)) {
                    pageBuilder.setNull(column);
                    return;
                }
                setter.apply(data);
            }
        });
        pageBuilder.addRecord();
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
         * We ignore record that has updated_at <= start_time
         */
        if (recordJsonNode.has(ZendeskConstants.Field.GENERATED_TIMESTAMP) && recordJsonNode.has(ZendeskConstants.Field.UPDATED_AT)) {
            final String recordUpdatedAtTime = recordJsonNode.get(ZendeskConstants.Field.UPDATED_AT).asText();
            final long recordUpdatedAtToEpochSecond = ZendeskDateUtils.isoToEpochSecond(recordUpdatedAtTime);
            return recordUpdatedAtToEpochSecond <= startTime;
        }

        return false;
    }

    private boolean isNull(final JsonNode jsonNode)
    {
        return jsonNode == null || jsonNode.isNull();
    }

    /*
     * For getting the timestamp value of the node
     * Sometime if the parser could not parse the value then return null
     * */
    private Timestamp getTimestampValue(final String value)
    {
        Timestamp result = null;
        try {
            final long timeStamp = ZendeskDateUtils.isoToEpochSecond(value);
            result = Timestamp.ofEpochSecond(timeStamp);
        }
        catch (final Exception e) {
            ZendeskBaseServices.logger.warn("Error when parse time stamp data " + value);
        }
        return result;
    }
}
