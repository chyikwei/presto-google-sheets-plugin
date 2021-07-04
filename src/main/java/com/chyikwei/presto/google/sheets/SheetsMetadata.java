package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class SheetsMetadata
        implements ConnectorMetadata
{
    private final SheetsClient client;

    @Inject
    public SheetsMetadata(SheetsClient client)
    {
        this.client = client;
    }

    private static final List<String> SCHEMAS = ImmutableList.of("default");

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMAS;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        //TODO: refactor
        Optional<SheetsTable> table = client.getSheetTable(tableName.getTableName());
        if (table.isPresent()) {
            return new SheetsTableHandle(tableName);
        }
        else {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        SheetsTableHandle tableHandle = (SheetsTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new SheetsTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SchemaTableName tableName = ((SheetsTableHandle) table).getSchemaTableName();
        Optional<SheetsTable> sheetsTable = client.getSheetTable(tableName.getTableName());
        if (sheetsTable.isPresent()) {
            return new ConnectorTableMetadata(tableName, sheetsTable.get().getColumnsMetadata());
        }
        else {
            throw new PrestoException(SheetsErrorCode.SHEETS_METASTORE_ERROR, "Metadata not found for table " + tableName.getTableName());
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        Optional<SheetsTable> table = client.getSheetTable(sheetsTableHandle.getSchemaTableName().getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(sheetsTableHandle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (SheetsColumnHandle column : table.get().getColumnHandles()) {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SheetsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        String schema = prefix.getSchemaName();
        for (SchemaTableName tableName : listTables(session, Optional.ofNullable(schema))) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, new SheetsTableHandle(tableName));
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schema = schemaName.orElse(getOnlyElement(SCHEMAS));
        if (listSchemaNames(session).contains(schema)) {
            return client.getTableNames().stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTableName() == null) {
            // No table name, get all schema tables
            return listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        // TODO: check table name
        return ImmutableList.of();
    }
}
