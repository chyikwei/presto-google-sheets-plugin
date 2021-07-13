package com.chyikwei.presto.google.sheets;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class SheetsConfig
{
    private String credentialsFilePath;
    private String driveFolderId;
    private int sheetsDataMaxCacheSize = 1000;
    private int sheetsDataMaxRow = 10000;
    private Duration sheetsDataExpireAfterWrite = new Duration(5, TimeUnit.MINUTES);

    @NotNull
    public String getCredentialsFilePath()
    {
        return credentialsFilePath;
    }

    @Config("credentials-path")
    @ConfigDescription("Credential file path to google service account")
    public SheetsConfig setCredentialsFilePath(String credentialsFilePath)
    {
        this.credentialsFilePath = credentialsFilePath;
        return this;
    }

    @NotNull
    public String getDriveFolderId()
    {
        return driveFolderId;
    }

    @Config("google-drive-folder-id")
    @ConfigDescription("Google drive folder id containing all tables")
    public SheetsConfig setDriveFolderId(String driveFolderId)
    {
        this.driveFolderId = driveFolderId;
        return this;
    }

    @Min(1)
    public int getSheetsDataMaxCacheSize()
    {
        return sheetsDataMaxCacheSize;
    }

    @Min(1)
    public int getSheetsDataMaxRow()
    {
        return sheetsDataMaxRow;
    }

    @Config("sheets-data-max-row")
    @ConfigDescription("Sheet data max row count. First row (column name) is not included")
    public SheetsConfig setSheetsDataMaxRow(int sheetsDataMaxRow)
    {
        this.sheetsDataMaxRow = sheetsDataMaxRow;
        return this;
    }

    @Config("sheets-data-max-cache-size")
    @ConfigDescription("Sheet data max cache size")
    public SheetsConfig setSheetsDataMaxCacheSize(int sheetsDataMaxCacheSize)
    {
        this.sheetsDataMaxCacheSize = sheetsDataMaxCacheSize;
        return this;
    }

    @MinDuration("1m")
    public Duration getSheetsDataExpireAfterWrite()
    {
        return sheetsDataExpireAfterWrite;
    }

    @Config("sheets-data-expire-after-write")
    @ConfigDescription("Sheets data expire after write duration")
    public SheetsConfig setSheetsDataExpireAfterWrite(Duration sheetsDataExpireAfterWriteMinutes)
    {
        this.sheetsDataExpireAfterWrite = sheetsDataExpireAfterWriteMinutes;
        return this;
    }
}
