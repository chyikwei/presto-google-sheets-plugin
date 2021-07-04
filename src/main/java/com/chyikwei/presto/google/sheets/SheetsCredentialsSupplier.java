package com.chyikwei.presto.google.sheets;

import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class SheetsCredentialsSupplier
{
    private static final List<String> SCOPES = ImmutableList.of(DriveScopes.DRIVE, SheetsScopes.SPREADSHEETS);
    private final Supplier<Credentials> credentialsCreator;

    public SheetsCredentialsSupplier(String credentialsFile)
    {
        // lazy creation, cache once it's created
        this.credentialsCreator = Suppliers.memoize(() -> {
            return createCredentialsFromFile(credentialsFile);
        });
    }

    private static Credentials createCredentialsFromFile(String file)
    {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(file)).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from file", e);
        }
    }

    Credentials getCredentials()
    {
        return credentialsCreator.get();
    }
}
