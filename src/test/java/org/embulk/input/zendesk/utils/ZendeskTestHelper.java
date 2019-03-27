package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.test.EmbulkTests;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.fail;

public final class ZendeskTestHelper
{
    private ZendeskTestHelper() {}

    private static ObjectMapper mapper;
    private static final ConfigLoader configLoader;

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        configLoader = new ConfigLoader(new ModelManager(null, mapper));
    }

    public static JsonNode getJsonFromFile(String fileName)
    {
        try {
            return mapper.readTree(EmbulkTests.readResource(fileName));
        }
        catch (IOException e) {
            fail("Fail to load file from " + fileName);
        }
        return null;
    }

    public static ConfigSource getConfigSource(String file)
    {
        ConfigSource configSource = null;
        try (final InputStream is = ZendeskTestHelper.class.getResourceAsStream("/config/" + file)) {
            configSource = configLoader.fromYaml(is);
        }
        catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
        return configSource;
    }
}
