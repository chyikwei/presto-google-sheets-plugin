package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SheetsTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private SheetsTableHandle tableHandle;

    @JsonCreator
    public SheetsTableLayoutHandle(@JsonProperty("table") SheetsTableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
    }

    @JsonProperty
    public SheetsTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
