package org.embulk.input.zendesk.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.ConfigException;

import java.util.Arrays;

public enum  ObjectType
{
    USER, TICKET, ARTICLE, ORGANIZATION, GROUP, CHAT, BRAND, ACCOUNT;

    @JsonCreator
    public static ObjectType fromString(final JsonNode jsonNode)
    {
        try {
            if (jsonNode.has("value")) {
                return ObjectType.valueOf(jsonNode.get("value").asText().toUpperCase());
            }
            throw new ConfigException("Object type config should follow format [value:value1,value:value2]");
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException("Unsupported ObjectType mode '" + jsonNode.get("value").asText() + "', supported values: '"
                    + Arrays.toString(ObjectType.values()) + "'");
        }
    }

    public String getStringType()
    {
        return "zen:" + this.name().toLowerCase();
    }
}
