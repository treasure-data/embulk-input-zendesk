package org.embulk.input.zendesk.models;

public class ZendeskErrorResponse
{
    public String error;
    public String description;

    @Override
    public String toString()
    {
        return "[error: " + error + ", description: " + description + "]";
    }
}
