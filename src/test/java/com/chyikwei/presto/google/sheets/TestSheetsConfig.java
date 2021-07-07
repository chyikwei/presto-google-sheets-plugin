package com.chyikwei.presto.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestSheetsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SheetsConfig.class)
                .setCredentialsFilePath(null)
                .setDriveFolderId(null)
                .setSheetsDataMaxCacheSize(1000)
                .setSheetsDataMaxRow(10000)
                .setSheetsDataExpireAfterWrite(new Duration(5, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("credentials-path", credentialsFile.toString())
                .put("google-drive-folder-id", "test_folder_id")
                .put("sheets-data-max-cache-size", "2000")
                .put("sheets-data-max-row", "100")
                .put("sheets-data-expire-after-write", "10m")
                .build();

        SheetsConfig expected = new SheetsConfig()
                .setCredentialsFilePath(credentialsFile.toString())
                .setDriveFolderId("test_folder_id")
                .setSheetsDataMaxCacheSize(2000)
                .setSheetsDataMaxRow(100)
                .setSheetsDataExpireAfterWrite(new Duration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
