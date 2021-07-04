package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import com.facebook.presto.testing.TestingConnectorContext;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;
import static com.chyikwei.presto.google.sheets.TestSheetsQuery.getTestCredentialsPath;
import static com.chyikwei.presto.google.sheets.TestSheetsQuery.GOOGLE_SHEETS;
import static com.chyikwei.presto.google.sheets.TestSheetsQuery.TEST_FOLER_ID;

class TestSheetsPlugin
{
    @Test
    public void testCreateConnector()
            throws Exception
    {
        Plugin plugin = new SheetsPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        ImmutableMap.Builder<String, String> propertiesMap = new ImmutableMap.Builder<String, String>()
                .put("credentials-path", getTestCredentialsPath())
                .put("google-drive-folder-id", TEST_FOLER_ID);
        Connector connector = factory.create(GOOGLE_SHEETS, propertiesMap.build(), new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }
}