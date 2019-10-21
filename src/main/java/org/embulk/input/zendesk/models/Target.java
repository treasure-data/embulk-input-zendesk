package org.embulk.input.zendesk.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

import java.util.Arrays;

public enum Target
{
    /** For ticket_metrics - we fetch by using include metric_sets with ticket target
    *    so the jsonName is different comparing to the target name
    */
    TICKETS("tickets"), USERS("users"), ORGANIZATIONS("organizations"), TICKET_EVENTS("ticket_events"),
    TICKET_METRICS("metric_sets"), TICKET_FIELDS("ticket_fields"), TICKET_FORMS("ticket_forms"),
    TICKET_METRIC_EVENTS("ticket_metric_events"), SATISFACTION_RATINGS("satisfaction_ratings"),
    RECIPIENTS("recipients"), SCORES("responses"), OBJECT_RECORDS("data"), RELATIONSHIP_RECORDS("data"), USER_EVENTS("data");

    String jsonName;

    Target(String jsonName)
    {
        this.jsonName = jsonName;
    }

    @JsonCreator
    public static Target fromString(final String value)
    {
        try {
            return Target.valueOf(value.trim().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException("Unsupported target '" + value + "', supported values: '"
                    + Arrays.toString(Target.values()) + "'");
        }
    }

    @Override
    public String toString()
    {
        return this.name().trim().toLowerCase();
    }

    public String getJsonName()
    {
        return this.jsonName;
    }
}
