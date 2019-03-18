package org.embulk.input.zendesk.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

import java.util.Arrays;

public enum Target
{
    /** For ticket_metrics - we fetch by using include metric_sets with ticket target
    *    so the jsonName is different comparing to the target name
    */
    TICKETS("tickets", "ticket"), USERS("users", "user"), ORGANIZATIONS("organizations", "organization"),
    TICKET_EVENTS("ticket_events", "ticket_event"), TICKET_METRICS("tickets", "metric_sets"),
    TICKET_FIELDS("ticket_fields", "ticket_field"), TICKET_FORMS("ticket_forms", "ticket_form");

    String jsonName;
    String singleFieldName;

    Target(String jsonName, String singleName)
    {
        this.jsonName = jsonName;
        this.singleFieldName = singleName;
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

    public String getSingleFieldName()
    {
        return this.singleFieldName;
    }
}
