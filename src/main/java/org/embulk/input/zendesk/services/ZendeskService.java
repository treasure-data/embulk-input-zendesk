package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.TaskReport;
import org.embulk.input.zendesk.RecordImporter;

public interface ZendeskService
{
    boolean isSupportIncremental();

    TaskReport addRecordToImporter(int taskIndex, RecordImporter recordImporter);

    JsonNode getData(String path, int page, boolean isPreview, long startTime);
}
