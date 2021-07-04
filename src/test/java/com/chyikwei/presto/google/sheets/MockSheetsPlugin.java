package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.util.Modules;

public class MockSheetsPlugin
        extends SheetsPlugin
{
    private final SheetsApiClient mockApiClient;
    private final Module extension = Modules.EMPTY_MODULE;

    public MockSheetsPlugin(SheetsApiClient mockApiClient)
    {
        this.mockApiClient = mockApiClient;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new MockSheetsConnectorFactory(mockApiClient, extension));
    }
}
