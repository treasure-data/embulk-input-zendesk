package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.embulk.EmbulkSystemProperties;
import org.embulk.GuiceBinder;
import org.embulk.TestPluginSourceModule;
import org.embulk.TestUtilityModule;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSourceImpl;
import org.embulk.config.ModelManager;
import org.embulk.exec.ExecModule;
import org.embulk.exec.ExtensionServiceLoaderModule;
import org.embulk.exec.SystemConfigModule;
import org.embulk.jruby.JRubyScriptingModule;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.ExecSessionInternal;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Properties;

/**
 * This is a clone from {@link org.embulk.EmbulkTestRuntime}, since there is no easy way to extend it.
 * The only modification is on the provided systemConfig, enable tests to run `embulk/guess/zendesk.rb`
 */
public class ZendeskPluginTestRuntime extends GuiceBinder
{
    public static class TestRuntimeModule implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            final EmbulkSystemProperties embulkSystemProperties = EmbulkSystemProperties.of(new Properties() { {
                setProperty("jruby_load_path", "lib");
            }});
            new SystemConfigModule(embulkSystemProperties).configure(binder);
            new ExecModule(embulkSystemProperties).configure(binder);
            new ExtensionServiceLoaderModule(embulkSystemProperties).configure(binder);
            new JRubyScriptingModule().configure(binder);
            new TestUtilityModule().configure(binder);
            new TestPluginSourceModule().configure(binder);
        }
    }

    private ExecSessionInternal exec;

    public ZendeskPluginTestRuntime()
    {
        super(new ZendeskPluginTestRuntime.TestRuntimeModule());
        Injector injector = getInjector();
        ConfigSource execConfig = new DataSourceImpl(injector.getInstance(ModelManager.class));
        this.exec = ExecSessionInternal.builderInternal(injector).fromExecConfig(execConfig).build();
    }

    public ExecSessionInternal getExec()
    {
        return exec;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        final Statement superStatement = ZendeskPluginTestRuntime.super.apply(base, description);
        return new Statement() {
            public void evaluate() throws Throwable
            {
                try {
                    ExecInternal.doWith(exec, (ExecAction<Void>) () -> {
                        try {
                            superStatement.evaluate();
                        }
                        catch (Throwable ex) {
                            throw new RuntimeExecutionException(ex);
                        }
                        return null;
                    });
                }
                catch (RuntimeException ex) {
                    throw ex.getCause();
                }
                finally {
                    exec.cleanup();
                }
            }
        };
    }

    private static class RuntimeExecutionException extends RuntimeException
    {
        public RuntimeExecutionException(Throwable cause)
        {
            super(cause);
        }
    }

    private static ConfigSource getSystemConfig()
    {
        ObjectNode configNode = JsonNodeFactory.instance.objectNode();
        configNode.set("jruby_load_path", JsonNodeFactory.instance.textNode("lib"));

        return new DataSourceImpl(new ModelManager(null, new ObjectMapper()), configNode);
    }
}
