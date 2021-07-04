package com.chyikwei.presto.google.sheets;

import com.google.api.services.drive.model.File;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestSheetsClient
{
    private static  String CREDENTIALS_FILE_PATH = "/Users/chyikwei/google_api_key/glassy-storm-314303-bc72df695b17.json";
    private static String FOLER_ID = "1d6DEQyl_GeXqSESD-ye56xIhGouxEpr0";

    private static Map<String, String> testTables = ImmutableMap.of(
            "test_sheet1", "19RKUNhq-3tOhEq3XvWx6HjHHbOEEFoz8Osd5hQsQJD0",
            "test_sheet2", "13APQJhPIYA-OGnvUn6VMQJSckJ6j1cM9s1v0Bkaz3wY"
        );

    private SheetsClient client;

    @BeforeClass
    public void initializeClient() {
        SheetsConfig config = new SheetsConfig()
                .setCredentialsFilePath(CREDENTIALS_FILE_PATH)
                .setDriveFolderId(FOLER_ID);
        SheetsCredentialsSupplier credentialsSupplier = new SheetsCredentialsSupplier(config.getCredentialsFilePath());
        SheetsApiClient sheetsApiClient = new SheetsApiClient(credentialsSupplier);
        client = new SheetsClient(config, sheetsApiClient);
    }

//    @Test
//    public void testReadFolder() {
//
//        List<File> files = client.getFiles();
//        assertEquals(files.size(), 2);
//
//        Map<String, String> testFiles = new HashMap<>();
//        for (File f : files) {
//            assertEquals(testTables.get(f.getName()), f.getId());
//        }
//    }

    @Test
    public void testReadFile() {
        Optional<SheetsTable> table = client.getSheetTable("test_sheet1");
        assertTrue(table.isPresent());

        List<List<Object>> data = table.get().getValues();
        assertTrue(data.size() > 0);
        int lineNo = 0;
        for (List<Object> lo : data) {
            System.out.println("Line " + lineNo);
            for (Object o : lo) {
                System.out.println((String) o);
            }
            lineNo += 1;
        }
    }
}