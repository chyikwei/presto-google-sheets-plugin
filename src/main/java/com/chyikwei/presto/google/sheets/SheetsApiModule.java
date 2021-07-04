package com.chyikwei.presto.google.sheets;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

public class SheetsApiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(SheetsApiClient.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public SheetsCredentialsSupplier provideSheetsCredentialsSupplier(SheetsConfig config)
    {
        return new SheetsCredentialsSupplier(config.getCredentialsFilePath());
    }
}
