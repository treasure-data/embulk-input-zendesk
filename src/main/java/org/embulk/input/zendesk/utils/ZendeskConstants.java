package org.embulk.input.zendesk.utils;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

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
        public static final String API = "/api/v2";
        public static final String API_INCREMENTAL = API + "/incremental";
        public static final String API_NPS_INCREMENTAL = API + "/nps/incremental";
        public static final String API_OBJECT_RECORD = "api/sunshine/objects/records";
        public static final String API_RELATIONSHIP_RECORD = "api/sunshine/relationships/records";
        public static final String API_USER_EVENT = "api/v2/users/%s/events";
        public static final String API_CHAT = API + "/chats";
        public static final String API_CHAT_SEARCH = API_CHAT + "/search";
    }

    public static class Misc
    {
        public static final String RUBY_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z";
        public static final String RUBY_TIMESTAMP_FORMAT_INPUT = "uuuu-MM-dd HH:mm:ss Z";
        public static final String JAVA_TIMESTAMP_FORMAT = "uuuu-MM-dd'T'HH:mm:ss.SSS'Z'";
        public static final String ISO_TIMESTAMP_FORMAT = "uuuu-MM-dd'T'HH:mm:ssXXX";
        public static final String ISO_INSTANT = "uuuu-MM-dd'T'HH:mm:ss'Z'";
        public static final String JAVA_TIMESTAMP_NANO_OF_SECOND = "uuuu-MM-dd'T'HH:mm:ss.n'Z'";
        public static final String RUBY_TIMESTAMP_FORMAT_INPUT_NO_SPACE = "uuuu-MM-dd HH:mm:ssZ";
        public static final String TOO_RECENT_START_TIME = "Too recent start_time.";
        public static final int RECORDS_SIZE_PER_PAGE = 100;
        public static final int MAXIMUM_RECORDS_INCREMENTAL = 1000;

        // 1 MB
        public static final int GUESS_BUFFER_SIZE = 1024 * 1024;
        public static final List<String> SUPPORT_DATE_TIME_FORMAT = ImmutableList.copyOf(Arrays.asList(ZendeskConstants.Misc.ISO_INSTANT, ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT,
                ZendeskConstants.Misc.JAVA_TIMESTAMP_FORMAT, ZendeskConstants.Misc.ISO_TIMESTAMP_FORMAT,
                ZendeskConstants.Misc.RUBY_TIMESTAMP_FORMAT_INPUT_NO_SPACE, ZendeskConstants.Misc.JAVA_TIMESTAMP_NANO_OF_SECOND));
    }

    public static class Regex
    {
        public static final String ID = "_id$";
        public static final String LOGIN_URL = "^https?://+[a-z0-9_\\\\-]+(.zendesk.com/?)$";
        public static final String CHAT_LOGIN_URL = "^https://www.zopim.com/?$";
    }

    public static class HttpStatus
    {
        public static final int TOO_MANY_REQUEST = 429;
    }
}
