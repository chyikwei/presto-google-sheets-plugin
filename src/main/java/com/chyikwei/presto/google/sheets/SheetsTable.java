package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class SheetsTable
{
    private final String name;
    private final String sheetsId;
    private final List<SheetsColumnHandle> columns;
    private final List<List<Object>> values;

    @JsonCreator
    public SheetsTable(
            @JsonProperty("name") String name,
            @JsonProperty("SheetsId") String sheetsId,
            @JsonProperty("columns") List<SheetsColumnHandle> columns,
            @JsonProperty("values") List<List<Object>> values)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        checkArgument(!isNullOrEmpty(sheetsId), "sheetsId is null or empty");
        requireNonNull(columns);

        this.name = name;
        this.sheetsId = sheetsId;
        this.columns = ImmutableList.copyOf(columns);
        this.values = values;
    }

    public List<List<Object>> getValues()
    {
        return this.values;
    }

    public List<SheetsColumnHandle> getColumnHandles()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (SheetsColumnHandle column : columns) {
            columnsMetadata.add(column.getColumnMetadata());
        }
        return columnsMetadata.build();
    }
}
