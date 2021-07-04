package com.chyikwei.presto.google.sheets;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SheetsConnectorFactory
        implements ConnectorFactory
{
    private final Module extension;

    SheetsConnectorFactory(Module module)
    {
        this.extension = requireNonNull(module, "extension is null");
    }

    @Override
    public String getName()
    {
        return "gsheets";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SheetsHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        try {
            Bootstrap app = new Bootstrap(
                    extension,
                    new JsonModule(),
                    new SheetsApiModule(),
                    new SheetsModule(context.getTypeManager()));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(SheetsConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
