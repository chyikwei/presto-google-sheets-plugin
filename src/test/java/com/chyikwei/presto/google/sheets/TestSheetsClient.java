package com.chyikwei.presto.google.sheets;

import com.google.api.services.drive.model.File;
import com.google.common.collect.ImmutableList;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestCredentialsPath;
import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestFolderId;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Listeners(MockitoTestNGListener.class)
public class TestSheetsClient
{
    @Mock SheetsApiClient apiClient;
    SheetsClient sheetsClient;
    private AutoCloseable closeable;

    private static final List<File> folderFiles = ImmutableList.of(
            new File().setName("dummy_table1").setId("test_sheets_id1"));

    private static final List<List<Object>> table1Data = ImmutableList.of(
            ImmutableList.of("id", "name"),
            ImmutableList.of("1", "xx"),
            ImmutableList.of("2", "yy"),
            ImmutableList.of("3", "zz"));

    @BeforeTest
    public void beforeTest() throws Exception
    {
        apiClient = mock(SheetsApiClient.class);
        closeable = MockitoAnnotations.openMocks(this);

        when(apiClient.readDriveFiles(getTestFolderId())).thenReturn(folderFiles);
        when(apiClient.readSheets("test_sheets_id1", "$1:$10000")).thenReturn(table1Data);

        SheetsConfig config = new SheetsConfig()
                .setCredentialsFilePath(getTestCredentialsPath())
                .setDriveFolderId(getTestFolderId());
        sheetsClient = new SheetsClient(config, apiClient);
    }

    @AfterTest
    public void afterTest() throws Exception
    {
        reset(apiClient);
        closeable.close();
    }

    @Test
    public void testGetTableNames()
    {
        List<String> tables = sheetsClient.getTableNames();
        assertEquals(tables, ImmutableList.of("dummy_table1"));
    }

    @Test
    public void testGetSheetTable()
    {
        Optional<SheetsTable> table = sheetsClient.getSheetTable("dummy_table1");
        assertTrue(table.isPresent());

        List<String> columnNames = table.get().getColumnHandles()
                .stream().map(SheetsColumnHandle::getColumnName)
                .collect(Collectors.toList());
        // check column name
        assertEquals(columnNames, table1Data.get(0));

        // check row count
        List<List<Object>> data = table.get().getValues();
        assertEquals(data.size(), 3);
    }
}
