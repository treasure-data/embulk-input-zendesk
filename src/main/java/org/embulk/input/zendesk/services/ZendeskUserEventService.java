package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.utils.URIBuilder;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.ZendeskInputPlugin;
import org.embulk.input.zendesk.models.Target;
import org.embulk.input.zendesk.stream.paginator.OrganizationSpliterator;
import org.embulk.input.zendesk.stream.paginator.UserEventSpliterator;
import org.embulk.input.zendesk.stream.paginator.UserSpliterator;
import org.embulk.input.zendesk.utils.ZendeskConstants;
import org.embulk.input.zendesk.utils.ZendeskDateUtils;
import org.embulk.input.zendesk.utils.ZendeskUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ZendeskUserEventService extends ZendeskBaseServices implements ZendeskService
{
    public ZendeskUserEventService(final ZendeskInputPlugin.PluginTask task)
    {
        super(task);
    }

    @Override
    protected String buildURI(final int page, final long startTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskReport execute(final ZendeskInputPlugin.PluginTask task, final int taskIndex, final Schema schema, final PageBuilder pageBuilder)
    {
        final TaskReport taskReport = Exec.newTaskReport();

        if (Exec.isPreview()) {
            JsonNode  jsonNode = mockJsonNode();
            addRecord(jsonNode.get(0), schema, pageBuilder);
        }
        else {
            final List<JsonNode> organizations = StreamSupport.stream(new OrganizationSpliterator(buildOrganizationURI(), getZendeskRestClient(), task), false)
                    .collect(Collectors.toList());
            final Set<String> knownUserIds = ConcurrentHashMap.newKeySet();
            final AtomicLong lastTime = new AtomicLong(0);
            organizations.parallelStream().forEach(
                    organization -> {
                        Stream<JsonNode> stream = StreamSupport.stream(new UserSpliterator(buildOrganizationWithUserURI(organization.get("url").asText()),
                                getZendeskRestClient(), task, Exec.isPreview()), true);

                        if (task.getDedup()) {
                            stream = stream.filter(item -> !knownUserIds.contains(item.get("id").asText()))
                                    .peek(item -> knownUserIds.add(item.get("id").asText()));
                        }
                        stream.forEach(s ->
                            StreamSupport.stream(new UserEventSpliterator(s.get("id").asText(), buildUserEventURI(s.get("id").asText()),
                                    getZendeskRestClient(), task, Exec.isPreview()), true)
                                    .forEach(item -> {
                                        if (task.getIncremental() && task.getEndTime().isPresent()) {
                                            long temp = ZendeskDateUtils.isoToEpochSecond(item.get("created_at").asText());
                                            if (temp > lastTime.get()) {
                                                // we need to store the created_at time of the latest records.
                                                // So we can calculate the start_time for configDiff in case there is no specific end_time
                                                lastTime.set(temp);
                                            }
                                        }
                                        addRecord(item, schema, pageBuilder);
                                    }));
                    }
            );
            storeStartTimeForConfigDiff(taskReport, lastTime.get());
        }

        return taskReport;
    }

    @Override
    public JsonNode getData(final String path, final int page, final boolean isPreview, final long startTime)
    {
        return new ObjectMapper().createObjectNode().set(task.getTarget().getJsonName(), mockJsonNode());
    }

    private String buildOrganizationURI()
    {
        return getURIBuilderFromHost()
                .setPath(ZendeskConstants.Url.API + "/" + Target.ORGANIZATIONS.getJsonName())
                .setParameter("per_page", "100")
                .setParameter("page", "1")
                .toString();
    }

    private String buildOrganizationWithUserURI(final String path)
    {
        return path.replace(".json", "")
                + "/users.json?per_page=100&page=1";
    }

    private String buildUserEventURI(final String userID)
    {
        final URIBuilder uriBuilder = getURIBuilderFromHost()
                .setPath(ZendeskConstants.Url.API_USER_EVENT)
                .setParameter("identifier", task.getProfileSource().get() + ":user_id:" + userID);

        task.getUserEventSource().ifPresent(eventSource -> uriBuilder.setParameter("source", eventSource));
        task.getUserEventType().ifPresent(eventType -> uriBuilder.setParameter("type", eventType));
        task.getStartTime().ifPresent(startTime -> uriBuilder.setParameter("start_time", ZendeskUtils.convertToDateTimeFormat(startTime, ZendeskConstants.Misc.ISO_INSTANT)));
        task.getEndTime().ifPresent(endTime -> uriBuilder.setParameter("end_time", ZendeskUtils.convertToDateTimeFormat(endTime, ZendeskConstants.Misc.ISO_INSTANT)));

        return uriBuilder.toString();
    }

    private JsonNode mockJsonNode()
    {
        try {
            String mockData = "[\n" +
                    "    {\n" +
                    "      \"id\": \"5c7f31aef8df240001e60bbf\",\n" +
                    "      \"type\": \"remove_from_cart\",\n" +
                    "      \"source\": \"shopify\",\n" +
                    "      \"description\": \"\",\n" +
                    "      \"authenticated\": true,\n" +
                    "      \"created_at\": \"2019-03-06T02:34:22Z\",\n" +
                    "      \"received_at\": \"2019-03-06T02:34:22Z\",\n" +
                    "      \"properties\": {\n" +
                    "        \"model\": 221,\n" +
                    "        \"size\": 6\n" +
                    "      },\n" +
                    "      \"user_id\": \"12312354234\"\n" +
                    "    }\n" +
                    "]";

            return new ObjectMapper().readTree(mockData);
        }
        catch (IOException ex) {
            throw new RuntimeException("Error when mock data for guess or preview " + ex.getMessage());
        }
    }
}
