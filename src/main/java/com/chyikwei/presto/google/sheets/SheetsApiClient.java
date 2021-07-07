package com.chyikwei.presto.google.sheets;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.LinkedList;
import java.util.List;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;

public class SheetsApiClient
{
    private static final Logger log = Logger.get(SheetsApiClient.class);
    private static final String APPLICATION_NAME = "presto google sheets integration";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    private final SheetsCredentialsSupplier credentialsSupplier;
    private final Supplier<Drive> driveSupplier;
    private final Supplier<Sheets> sheetsSupplier;

    @Inject
    public SheetsApiClient(SheetsCredentialsSupplier credentialsSupplier)
    {
        this.credentialsSupplier = credentialsSupplier;

        this.driveSupplier = Suppliers.memoize(() -> createDriveFromCredentials(credentialsSupplier));

        this.sheetsSupplier = Suppliers.memoize(() -> createSheetsFromCredentials(credentialsSupplier));
    }

    private static Drive createDriveFromCredentials(SheetsCredentialsSupplier credentialsSupplier)
    {
        try {
            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentialsSupplier.getCredentials());
            return new Drive.Builder(newTrustedTransport(), JSON_FACTORY, requestInitializer).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    private static Sheets createSheetsFromCredentials(SheetsCredentialsSupplier credentialsSupplier)
    {
        try {
            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentialsSupplier.getCredentials());
            return new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, requestInitializer).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    public List<File> readDriveFiles(String folderId)
    {
        List<File> files = new LinkedList<>();
        try {
            String pageToken = null;
            if (log.isDebugEnabled()) {
                log.debug("loading table list from folder: {}", folderId);
            }
            do {
                FileList result = this.driveSupplier.get().files().list()
                        .setQ(String.format("parents = '{}'", folderId))
                        .setQ("mimeType = 'application/vnd.google-apps.spreadsheet'")
                        .setSpaces("drive")
                        .setFields("nextPageToken, files(id, name)")
                        .setPageToken(pageToken)
                        .execute();

                files.addAll(result.getFiles());
                pageToken = result.getNextPageToken();
            }
            while (pageToken != null);
            return files;
        }
        catch (IOException e) {
            throw new PrestoException(SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR, e);
        }
    }

    public List<List<Object>> readSheets(String sheetsId, int maxRow)
    {
        String range = String.format("$1:${}", maxRow);
        return readSheets(sheetsId, range);
    }

    public List<List<Object>> readSheets(String sheetsId, String range)
    {
        try {
            //String defaultRange = "$1:$10000";
            if (log.isDebugEnabled()) {
                log.debug("loading sheets {} with range {}", sheetsId, range);
            }
            // Notes: read first tab by default
            return sheetsSupplier.get().spreadsheets().values().get(sheetsId, range).execute().getValues();
        }
        catch (IOException e) {
            throw new PrestoException(SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR, "Failed loading sheet " + sheetsId);
        }
    }
}
