package org.embulk.input.zendesk.utils;

public class ZendeskConstants
{
    private ZendeskConstants()
    {
    }

    public static class Header
    {
        public static final String APPLICATION_JSON = "application/json";

        public static final String ZENDESK_MARKETPLACE_NAME = "X-Zendesk-Marketplace-Name";
        public static final String ZENDESK_MARKETPLACE_ORGANIZATION_ID = "X-Zendesk-Marketplace-Organization-Id";
        public static final String ZENDESK_MARKETPLACE_APP_ID = "X-Zendesk-Marketplace-App-Id";
    }

    public static class Field
    {
        public static final String START_TIME = "start_time";
        public static final String END_TIME = "end_time";
        public static final String COUNT = "count";
        public static final String GENERATED_TIMESTAMP = "generated_timestamp";
        public static final String UPDATED_AT = "updated_at";
        public static final String ID = "id";
    }

    public static class Url
    {
        public static final String API = "api/v2";
        public static final String API_INCREMENTAL = API + "/incremental";
    }

    public static class Misc
    {
        public static final String RUBY_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z";
        public static final String RUBY_TIMESTAMP_FORMAT_INPUT = "yyyy-MM-dd HH:mm:ss Z";
        public static final String JAVA_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        public static final String ISO_INSTANT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        public static final String TOO_RECENT_START_TIME = "Too recent start_time.";

        public static final int RECORDS_SIZE_PER_PAGE = 100;
        public static final int MAXIMUM_RECORDS_INCREMENTAL = 1000;

        // 1 MB
        public static final int GUESS_BUFFER_SIZE = 1024 * 1024;
    }

    public static class Regex
    {
        public static final String ID = "_id$";
        public static final String HOST = "^(https:\\/\\/)?(www.)?([a-zA-Z0-9]+).zendesk.com/$";
    }
}
