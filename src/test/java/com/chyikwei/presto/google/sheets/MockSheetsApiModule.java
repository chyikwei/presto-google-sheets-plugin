package com.chyikwei.presto.google.sheets;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

public class MockSheetsApiModule extends SheetsApiModule
{
    private final SheetsApiClient mockApiClient;
    public MockSheetsApiModule(SheetsApiClient mockApiClient) {
        this.mockApiClient = mockApiClient;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(SheetsApiClient.class).toInstance(mockApiClient);
    }
}
