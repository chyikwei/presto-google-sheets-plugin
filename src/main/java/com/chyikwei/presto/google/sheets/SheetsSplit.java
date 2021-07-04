package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public class SheetsSplit
        implements ConnectorSplit
{
    private final SchemaTableName schemaTableName;
    private final List<List<Object>> values;

    @JsonCreator
    public SheetsSplit(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("values") List<List<Object>> values)
    {
        this.schemaTableName = schemaTableName;
        this.values = values;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<List<Object>> getValues()
    {
        return values;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("schemaTableName", schemaTableName)
                .build();
    }
}
