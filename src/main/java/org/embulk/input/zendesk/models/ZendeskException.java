package org.embulk.input.zendesk.models;

public class ZendeskException extends Exception
{
    private static final long serialVersionUID = -256731723520584046L;
    private final int statusCode;
    private final int retryAfter;

    public ZendeskException(int statusCode, String message, int retryAfter)
    {
        super(message);
        this.statusCode = statusCode;
        this.retryAfter = retryAfter;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public int getRetryAfter()
    {
        return retryAfter;
    }
}
