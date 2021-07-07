package com.chyikwei.presto.google.sheets;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.api.services.drive.model.File;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.util.List;

import static com.chyikwei.presto.google.sheets.utils.DummyConfig.GOOGLE_SHEETS;
import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestCredentialsPath;
import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestFolderId;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Listeners(MockitoTestNGListener.class)
public class TestSheetsQuery
        extends AbstractTestQueryFramework
{
    @Mock SheetsApiClient apiClient;

    private AutoCloseable closeable;

    private static final List<File> folderFiles = ImmutableList.of(
        new File().setName("dummy_table1").setId("test_sheets_id1"));

    private static final List<List<Object>> table1Data = ImmutableList.of(
            ImmutableList.of("id", "name"),
            ImmutableList.of("1", "xx"),
            ImmutableList.of("2", "yy"),
            ImmutableList.of("3", "zz"));

    @BeforeTest
    public void beforeTest()
    {
        apiClient = mock(SheetsApiClient.class);
        closeable = MockitoAnnotations.openMocks(this);
        when(apiClient.readDriveFiles(getTestFolderId())).thenReturn(folderFiles);
        when(apiClient.readSheets("test_sheets_id1", 10001)).thenReturn(table1Data);
    }

    @AfterTest
    public void afterTest() throws Exception
    {
        reset(apiClient);
        closeable.close();
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            queryRunner.installPlugin(new MockSheetsPlugin(apiClient));
            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, ImmutableMap.of(
                    "credentials-path", getTestCredentialsPath(),
                    "google-drive-folder-id", getTestFolderId(),
                    "sheets-data-max-cache-size", "1000",
                    "sheets-data-expire-after-write", "5m"));
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    @Test
    public void testListTable()
    {
        assertQuery("show tables", "SELECT * FROM (VALUES 'dummy_table1')");
        assertQueryReturnsEmptyResult("SHOW TABLES IN gsheets.information_schema LIKE 'dummy_table1'");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema'", "SELECT * FROM (VALUES 'dummy_table1')");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema' LIMIT 1000", "SELECT * FROM (VALUES 'dummy_table1')");
        assertEquals(getQueryRunner().execute("select table_name from gsheets.information_schema.tables WHERE table_schema = 'unknown_schema'").getRowCount(), 0);
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM dummy_table1", "SELECT 3");
        assertQuery("SELECT id FROM dummy_table1", "SELECT * FROM (VALUES '1', '2', '3')");
        assertQuery("SELECT name FROM dummy_table1", "SELECT * FROM (VALUES 'xx','yy', 'zz')");
    }
}
