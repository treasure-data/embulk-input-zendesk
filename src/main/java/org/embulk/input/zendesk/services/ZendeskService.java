package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.TaskReport;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

public interface ZendeskService
{
    TaskReport execute(int taskIndex, Schema schema, PageBuilder pageBuilder);

    JsonNode getData(String path, int page, boolean isPreview, long startTime);
}
