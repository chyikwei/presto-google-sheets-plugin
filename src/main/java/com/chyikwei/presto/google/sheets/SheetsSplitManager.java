package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SheetsSplitManager
        implements ConnectorSplitManager
{
    private SheetsClient sheetsClient;

    @Inject
    public SheetsSplitManager(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        SheetsTableHandle tableHandle = ((SheetsTableLayoutHandle) layout).getTableHandle();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        Optional<SheetsTable> table = sheetsClient.getSheetTable(tableName);
        if (table.isPresent()) {
            List<List<Object>> data = table.get().getValues();
            List<SheetsSplit> splits = new ArrayList<>();
            splits.add(new SheetsSplit(tableHandle.getSchemaTableName(), data));
            return new FixedSplitSource(splits);
        }
        else {
            throw new PrestoException(SheetsErrorCode.SHEETS_TABLE_EMPTY_ERROR, "Failed reading table: " + tableName);
        }
    }
}
