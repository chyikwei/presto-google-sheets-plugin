package com.chyikwei.presto.google.sheets;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;


public class TestSheets extends AbstractTestQueryFramework
{
    protected static final String GOOGLE_SHEETS = "gsheets";
    private static  String CREDENTIALS_FILE_PATH = "/Users/chyikwei/google_api_key/glassy-storm-314303-bc72df695b17.json";
    private static String FOLER_ID = "1d6DEQyl_GeXqSESD-ye56xIhGouxEpr0";

    private static Map<String, String> testTables = ImmutableMap.of(
            "test_sheet1", "19RKUNhq-3tOhEq3XvWx6HjHHbOEEFoz8Osd5hQsQJD0",
            "test_sheet2", "13APQJhPIYA-OGnvUn6VMQJSckJ6j1cM9s1v0Bkaz3wY"
    );

    @Override
    protected  QueryRunner createQueryRunner() {
        QueryRunner queryRunner;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            queryRunner.installPlugin(new SheetsPlugin());
            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, ImmutableMap.of(
                    "credentials-path", CREDENTIALS_FILE_PATH,
                    "google-drive-folder-id", FOLER_ID,
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
        assertQuery("show tables", "SELECT * FROM (VALUES 'test_sheet1', 'test_sheet2')");
        assertQueryReturnsEmptyResult("SHOW TABLES IN gsheets.information_schema LIKE 'test_sheet1'");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema'", "SELECT * FROM (VALUES 'test_sheet1', 'test_sheet2')");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema' LIMIT 1000", "SELECT * FROM (VALUES 'test_sheet1', 'test_sheet2')");
        assertEquals(getQueryRunner().execute("select table_name from gsheets.information_schema.tables WHERE table_schema = 'unknown_schema'").getRowCount(), 0);
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM test_sheet1", "SELECT 6");
        assertQuery("SELECT id FROM test_sheet1", "SELECT * FROM (VALUES '0', '1','2','3',null,'5')");
        assertQuery("SELECT name FROM test_sheet1", "SELECT * FROM (VALUES 'aaa','bbb','ccc','ddd','eee', 'fff')");
        //assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES ('1','one'), ('2','two'), ('3','three'), ('4','four'), ('5','five'))");
    }

    @Test
    public void testReadEmptyTable() {
        assertQueryFails("SELECT * from test_sheet2", ".*Table gsheets.default.test_sheet2 does not exist");

    }
}
