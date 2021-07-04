package com.chyikwei.presto.google.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

import java.util.List;
import java.util.stream.Collectors;

public class SheetsRecordSet
        implements RecordSet
{
    private final List<SheetsColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<List<Object>> values;

    public SheetsRecordSet(List<SheetsColumnHandle> columnHandles, List<List<Object>> values)
    {
        this.columnHandles = columnHandles;
        this.values = values;
        this.columnTypes = columnHandles.stream().map(SheetsColumnHandle::getColumnType).collect(Collectors.toList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new SheetsRecordCursor(columnHandles, values);
    }
}
