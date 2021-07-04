package com.chyikwei.presto.google.sheets;

import com.chyikwei.presto.google.sheets.SheetsApiClient;
import com.chyikwei.presto.google.sheets.SheetsConnectorFactory;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class MockSheetsConnectorFactory extends SheetsConnectorFactory
{
    private final SheetsApiClient mockApiClient;
    MockSheetsConnectorFactory(SheetsApiClient mockApiClient, Module extension) {
        super(extension);
        this.mockApiClient = mockApiClient;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MockSheetsApiModule(mockApiClient),
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
