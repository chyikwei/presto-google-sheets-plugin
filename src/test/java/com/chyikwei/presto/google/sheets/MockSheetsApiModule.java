package com.chyikwei.presto.google.sheets;

import com.google.inject.Binder;

public class MockSheetsApiModule
        extends SheetsApiModule
{
    private final SheetsApiClient mockApiClient;

    public MockSheetsApiModule(SheetsApiClient mockApiClient)
    {
        this.mockApiClient = mockApiClient;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(SheetsApiClient.class).toInstance(mockApiClient);
    }
}
