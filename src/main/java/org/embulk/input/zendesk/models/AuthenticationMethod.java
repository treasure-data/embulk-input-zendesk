package org.embulk.input.zendesk.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

import java.util.Arrays;

public enum AuthenticationMethod
{
    BASIC, OAUTH, TOKEN;

    @JsonCreator
    public static AuthenticationMethod fromString(final String value)
    {
        try {
            return AuthenticationMethod.valueOf(value.trim().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException("Unsupported Authentication mode '" + value + "', supported values: '"
                    + Arrays.toString(Target.values()) + "'");
        }
    }
}
