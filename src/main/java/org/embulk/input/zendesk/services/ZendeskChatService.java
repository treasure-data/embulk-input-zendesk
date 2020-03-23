package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.clients.ZendeskRestClient;

import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

public class ZendeskChatService implements ZendeskService
{
    private static final Logger logger = LoggerFactory.getLogger(ZendeskChatService.class);

    private ZendeskRestClient zendeskRestClient;

    protected ZendeskInputPlugin.PluginTask task;

    private static final int MAXIMUM_TOTAL_RECORDS = 10000;

    private static final int MAXIMUM_TOTAL_PAGES = 250;

    private static final int MAXIMUM_RECORDS_PER_PAGE = 40;

    public ZendeskChatService(final ZendeskInputPlugin.PluginTask task)
    {
        this.task = task;
    }

    @Override
    public boolean isSupportIncremental()
    {
        return true;
    }

    @Override
    public TaskReport addRecordToImporter(final int taskIndex, final RecordImporter recordImporter)
    {
        final TaskReport taskReport = Exec.newTaskReport();
        String startTime = getStartTime();
        String endTime = getEndTime();

        if (Exec.isPreview()) {
            fetchData(startTime, endTime, 1, recordImporter);
            return taskReport;
        }

        while (true) {
            String searchURI = buildSearchRequest(startTime, endTime, 1);

            String response = getZendeskRestClient().doGet(searchURI, task, Exec.isPreview());

            JsonNode json = ZendeskUtils.parseJsonObject(response);

            if (!ZendeskUtils.isNull(json)) {
                int totalRecords = json.get("count").asInt();
                int totalPages = totalRecords > MAXIMUM_TOTAL_RECORDS
                    ? MAXIMUM_TOTAL_PAGES
                    : slicePages(totalRecords);

                logger.info(String.format("Fetching from '%s' to '%s' with '%d' pages and '%d' records",
                    startTime, endTime, totalPages,  totalRecords));

                if (totalPages > 1) {
                    final String lastEndTime = endTime;
                    // Can't start from 0 because page 0 and 1 return the same data, and according to document, page start from 1
                    IntStream.range(1, totalPages + 1).parallel().forEach(page -> fetchData(startTime, lastEndTime, page, recordImporter));
                }
                else {
                    // in case totalPages 1, IntStream not work
                    fetchData(startTime, endTime, 1, recordImporter);
                }

                if (totalRecords <= MAXIMUM_TOTAL_RECORDS || Exec.isPreview()) {
                    break;
                }

                endTime = getNextEndDate(startTime, endTime);
            }
        }

        storeStartTimeForConfigDiff(taskReport,
            startTime.equals("0") ? 0 : ZendeskDateUtils.isoToEpochSecond(startTime),
            ZendeskDateUtils.isoToEpochSecond(endTime));
        return taskReport;
    }

    @Override
    public JsonNode getDataFromPath(final String path, final int page, final boolean isPreview, final long initTime)
    {
        String startTime = getStartTime();
        String endTime = getEndTime();

        List<String> ids = getListIDS(startTime, endTime, 1, true);

        if (ids.size() > 0) {
            String fetchIdsURI = buildSearchRequest(ids);

            String response = zendeskRestClient.doGet(fetchIdsURI, task, true);

            return new ObjectMapper().createObjectNode().set(task.getTarget().getJsonName(), parseData(response));
        }

        return new ObjectMapper().createObjectNode().set(task.getTarget().getJsonName(), new ObjectMapper().createArrayNode());
    }

    protected void fetchData(final String startTime, final String endTime, final int page, final RecordImporter recordImporter)
    {
        List<String> ids = getListIDS(startTime, endTime, page, Exec.isPreview());

        if (ids.size() > 0) {
            String fetchIdsURI = buildSearchRequest(ids);

            String response = getZendeskRestClient().doGet(fetchIdsURI, task, false);

            JsonNode data = ZendeskUtils.parseJsonObject(response);
            if (data.get("count").asInt() > 0) {
                data.get("docs").forEach(item -> {
                    if (item != null) {
                        recordImporter.addRecord(item);
                    }
                });
            }
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

    private List<String> getListIDS(final String startTime, final String endTime, final int page, final boolean isPreview)
    {
        String searchURI = buildSearchRequest(startTime, endTime, page);

        String response = getZendeskRestClient().doGet(searchURI, task, isPreview);
        JsonNode json = ZendeskUtils.parseJsonObject(response);
        Iterator<JsonNode> data = ZendeskUtils.getListRecords(json, "results");

        List<String> ids = new ArrayList<>();
        data.forEachRemaining(record -> ids.add(record.get("id").asText()));
        return ids;
    }

    private String buildSearchRequest(final String startTime, final String endTime, final int page)
    {
        return ZendeskUtils.getURIBuilder(task.getLoginUrl())
            .setPath(ZendeskConstants.Url.API_CHAT_SEARCH)
            .setParameter("q", buildSearchParam(startTime, endTime))
            .setParameter("page", String.valueOf(page)).toString();
    }

    private String buildSearchParam(final String startTime, final String endTime)
    {
        return new StringBuilder()
            .append("timestamp:[")
            .append(
                startTime.equals("0")
                ? "*"
                : ZendeskDateUtils.convertToDateTimeFormat(startTime, ZendeskConstants.Misc.ISO_INSTANT))
            .append(" TO ")
            .append(ZendeskDateUtils.convertToDateTimeFormat(endTime, ZendeskConstants.Misc.ISO_INSTANT))
            .append("]")
            .toString();
    }

    private String buildSearchRequest(final List<String> ids)
    {
        return ZendeskUtils.getURIBuilder(task.getLoginUrl())
            .setPath(ZendeskConstants.Url.API_CHAT)
            .setParameter("ids", buildIdsParam(ids))
            .toString();
    }

    private String buildIdsParam(final List<String> ids)
    {
        return Joiner.on(",").join(ids);
    }

    private ArrayNode parseData(String response)
    {
        ArrayNode arrayNode = new ObjectMapper().createArrayNode();
        JsonNode data = ZendeskUtils.parseJsonObject(response);
        if (data.get("count").asInt() > 0) {
            data.get("docs").forEach(item -> {
                if (item != null) {
                    arrayNode.add(item);
                }
            });
        }
        return arrayNode;
    }

    private int slicePages(final int totalRecords)
    {
        if (totalRecords % MAXIMUM_RECORDS_PER_PAGE == 0) {
            return totalRecords / MAXIMUM_RECORDS_PER_PAGE;
        }
        return totalRecords / MAXIMUM_RECORDS_PER_PAGE + 1;
    }

    private String getNextEndDate(final String startTime, final String endTime)
    {
        String searchURI = buildSearchRequest(startTime, endTime, MAXIMUM_TOTAL_PAGES);
        String response = getZendeskRestClient().doGet(searchURI, task, false);
        JsonNode json = ZendeskUtils.parseJsonObject(response);

        return ZendeskDateUtils.convertToDateTimeFormat(
                    json.get("results")
                    .get(MAXIMUM_RECORDS_PER_PAGE - 1)
                    .get("timestamp").asText(), ZendeskConstants.Misc.ISO_INSTANT);
    }

    private void storeStartTimeForConfigDiff(final TaskReport taskReport, final long initStartTime, final long resultEndTime)
    {
        if (task.getIncremental()) {
            long nextStartTime;
            long now = Instant.now().getEpochSecond();
            // no record to add
            if (resultEndTime == 0) {
                nextStartTime = now;
            }
            else {
                if (task.getEndTime().isPresent()) {
                    long endTime = ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get());
                    nextStartTime = endTime + 1;
                }
                else {
                    // NOTE: start_time compared as "=>", not ">".
                    // If we will use end_time for next start_time, we got the same records that are fetched
                    // end_time + 1 is workaround for that
                    nextStartTime = resultEndTime + 1;
                }
            }

            if (task.getEndTime().isPresent()) {
                long endTime = ZendeskDateUtils.isoToEpochSecond(task.getEndTime().get());
                taskReport.set(ZendeskConstants.Field.END_TIME, nextStartTime + endTime - initStartTime);
            }

            taskReport.set(ZendeskConstants.Field.START_TIME, nextStartTime);
        }
    }

    private String getStartTime()
    {
        return task.getStartTime().isPresent()
            ? task.getStartTime().get()
            : "0";
    }

    private String getEndTime()
    {
        return task.getEndTime().isPresent()
            ? task.getEndTime().get()
            : OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }
}
