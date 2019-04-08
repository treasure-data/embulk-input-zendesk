package org.embulk.input.zendesk.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.embulk.GuiceBinder;
import org.embulk.RandomManager;
import org.embulk.TestPluginSourceModule;
import org.embulk.TestUtilityModule;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSourceImpl;
import org.embulk.config.ModelManager;
import org.embulk.exec.ExecModule;
import org.embulk.exec.ExtensionServiceLoaderModule;
import org.embulk.exec.SystemConfigModule;
import org.embulk.jruby.JRubyScriptingModule;
import org.embulk.plugin.BuiltinPluginSourceModule;
import org.embulk.plugin.PluginClassLoaderFactory;
import org.embulk.plugin.PluginClassLoaderModule;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Random;

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
            ConfigSource systemConfig = getSystemConfig();
            new SystemConfigModule(systemConfig).configure(binder);
            new ExecModule().configure(binder);
            new ExtensionServiceLoaderModule(systemConfig).configure(binder);
            new BuiltinPluginSourceModule().configure(binder);
            new JRubyScriptingModule(systemConfig).configure(binder);
            new PluginClassLoaderModule(systemConfig).configure(binder);
            new TestUtilityModule().configure(binder);
            new TestPluginSourceModule().configure(binder);
        }
    }

    private ExecSession exec;

    public ZendeskPluginTestRuntime()
    {
        super(new TestRuntimeModule());
        Injector injector = getInjector();
        ConfigSource execConfig = new DataSourceImpl(injector.getInstance(ModelManager.class));
        this.exec = ExecSession.builder(injector).fromExecConfig(execConfig).build();
    }

    public ExecSession getExec()
    {
        return exec;
    }

    public BufferAllocator getBufferAllocator()
    {
        return getInstance(BufferAllocator.class);
    }

    public ModelManager getModelManager()
    {
        return getInstance(ModelManager.class);
    }

    public Random getRandom()
    {
        return getInstance(RandomManager.class).getRandom();
    }

    public PluginClassLoaderFactory getPluginClassLoaderFactory()
    {
        return getInstance(PluginClassLoaderFactory.class);
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        final Statement superStatement = ZendeskPluginTestRuntime.super.apply(base, description);
        return new Statement() {
            public void evaluate() throws Throwable
            {
                try {
                    Exec.doWith(exec, (ExecAction<Void>) () -> {
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
        configNode.set("jruby_load_path", JsonNodeFactory.instance.arrayNode().add("lib"));

        return new DataSourceImpl(new ModelManager(null, new ObjectMapper()), configNode);
    }
}
