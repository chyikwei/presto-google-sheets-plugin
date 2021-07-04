package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.util.Modules;

public class SheetsPlugin
        implements Plugin
{
    private final Module module;

    public SheetsPlugin()
    {
        this(Modules.EMPTY_MODULE);
    }

    public SheetsPlugin(Module module)
    {
        this.module = module;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new SheetsConnectorFactory(module));
    }
}
