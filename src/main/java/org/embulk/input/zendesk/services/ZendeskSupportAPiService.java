package org.embulk.input.zendesk.services;

import com.fasterxml.jackson.databind.JsonNode;

public interface ZendeskSupportAPiService
{
    JsonNode getData(String path, int page, boolean isPreview);
}
