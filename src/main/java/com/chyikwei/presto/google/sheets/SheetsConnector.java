package com.chyikwei.presto.google.sheets;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

public class SheetsConnector
        implements Connector
{
    private static final Logger log = Logger.get(SheetsConnector.class);
    private final SheetsMetadata metadata;
    private final SheetsSplitManager splitManager;
    private final SheetsRecordSetProvider recordSetProvider;

    @Inject
    public SheetsConnector(
            SheetsMetadata metadata,
            SheetsSplitManager splitManager,
            SheetsRecordSetProvider recordSetProvider)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return SheetsTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }
}
