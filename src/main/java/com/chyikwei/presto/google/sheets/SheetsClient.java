package com.chyikwei.presto.google.sheets;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.google.api.services.drive.model.File;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SheetsClient
{
    private static final Logger log = Logger.get(SheetsClient.class);

    private final String folderId;
    private final int sheetsMaxDataRow;
    private final SheetsApiClient sheetsApiClient;

    private Map<String, String> tableSheetIdMap = ImmutableMap.of();
    private List<String> tableNames = ImmutableList.of();

    @Inject
    public SheetsClient(SheetsConfig config, SheetsApiClient sheetsApiClient)
    {
        this.folderId = config.getDriveFolderId();
        this.sheetsMaxDataRow = config.getSheetsDataMaxRow();
        this.sheetsApiClient = sheetsApiClient;
    }

    private void refreshTableMeta()
    {
        Map<String, String> tableSheetIdMap = new HashMap<>();
        List<String> tableNames = new ArrayList<>();

        List<File> files = sheetsApiClient.readDriveFiles(folderId);
        for (File file : files) {
            String fileName = file.getName();
            if (tableSheetIdMap.containsKey((fileName))) {
                throw new PrestoException(SheetsErrorCode.SHEETS_METASTORE_ERROR, "duplicate table name: " + fileName);
            }
            else {
                tableSheetIdMap.put(fileName, file.getId());
                tableNames.add(fileName);
            }
        }
        this.tableSheetIdMap = ImmutableMap.copyOf(tableSheetIdMap);
        Collections.sort(tableNames);
        this.tableNames = ImmutableList.copyOf(tableNames);
    }

    public List<String> getTableNames()
    {
        if (this.tableNames.size() == 0) {
            refreshTableMeta();
        }
        return this.tableNames;
    }

    public Optional<SheetsTable> getSheetTable(String tableName)
    {
        if (tableSheetIdMap.size() == 0) {
            refreshTableMeta();
        }

        String sheetsId = tableSheetIdMap.get(tableName);
        if (sheetsId == null) {
            log.debug("not sheetsId for table {}", tableName);
            return Optional.empty();
        }
        //TODO
        log.info("call sheets client with maxRow: " + (sheetsMaxDataRow + 1));
        List<List<Object>> sheetData = sheetsApiClient.readSheets(sheetsId, sheetsMaxDataRow + 1);
        if (sheetData == null || sheetData.size() == 0) {
            return Optional.empty();
        }
        List<SheetsColumnHandle> columns = parseColumns(sheetData.get(0));
        List<List<Object>> values = sheetData.subList(1, sheetData.size());
        log.info("Table %s, %d columns, %d rows", tableName, columns.size(), values.size());
        SheetsTable table = new SheetsTable(tableName, sheetsId, columns, values);
        return Optional.of(table);
    }

    private List<SheetsColumnHandle> parseColumns(List<Object> header)
    {
        ImmutableList.Builder<SheetsColumnHandle> columns = ImmutableList.builder();
        int colIndex = 0;
        Set<String> uniqueColNames = new HashSet<>();
        for (Object c : header) {
            String colName = c.toString().toLowerCase(Locale.ENGLISH).replace(' ', '_');
            if (colName.isEmpty() || uniqueColNames.contains(colName)) {
                colName = "column_" + colIndex;
            }
            uniqueColNames.add(colName);
            log.info("add column: %s", colName);
            columns.add(new SheetsColumnHandle(colName, VarcharType.VARCHAR, colIndex));
            colIndex += 1;
        }
        return columns.build();
    }
}
