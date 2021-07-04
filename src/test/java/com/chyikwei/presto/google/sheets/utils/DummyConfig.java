package com.chyikwei.presto.google.sheets.utils;

import com.chyikwei.presto.google.sheets.TestSheetsQuery;

import java.net.URL;
import java.nio.file.Paths;

public class DummyConfig
{
    public static final String GOOGLE_SHEETS = "gsheets";
    private static final String TEST_FOLDER_ID = "test_folder_id";
    private static final String TEST_CREDENTIAL_FILE = "test-account.json";

    private DummyConfig() {}

    public static String getTestCredentialsPath()
            throws Exception
    {
        URL res = TestSheetsQuery.class.getClassLoader().getResource(TEST_CREDENTIAL_FILE);
        java.io.File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }

    public static String getTestFolderId()
    {
        return TEST_FOLDER_ID;
    }
}
