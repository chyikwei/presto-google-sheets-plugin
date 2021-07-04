package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.chyikwei.presto.google.sheets.utils.DummyConfig.GOOGLE_SHEETS;
import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestCredentialsPath;
import static com.chyikwei.presto.google.sheets.utils.DummyConfig.getTestFolderId;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;

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
                .put("google-drive-folder-id", getTestFolderId());
        Connector connector = factory.create(GOOGLE_SHEETS, propertiesMap.build(), new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }
}
