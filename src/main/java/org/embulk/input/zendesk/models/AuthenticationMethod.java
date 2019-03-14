package org.embulk.input.zendesk.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

public enum AuthenticationMethod
{
    BASIC, OAUTH, TOKEN;

    @JsonCreator
    public static AuthenticationMethod fromString(final String value)
    {
        final String normalizedValue = value.trim().toLowerCase();

        switch (normalizedValue) {
            case "basic":
                return AuthenticationMethod.BASIC;
            case "oauth":
                return AuthenticationMethod.OAUTH;
            case "token":
                return AuthenticationMethod.TOKEN;
            default:
                throw new ConfigException(String.format("Authentication mode %s is not supported", value));
        }
    }
}
