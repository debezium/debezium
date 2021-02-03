/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

public class OracleConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorConfigTest.class);

    @Test
    public void validXtreamNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertTrue(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.SCHEMA_NAME, "myschema")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertTrue(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validXtreamWithUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.URL, "jdbc:oci:thin:@myserver/mydatabase")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertTrue(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerWithUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.URL, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.SCHEMA_NAME, "myschema")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertTrue(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validUrlTNS() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.URL,
                                "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.11)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.12)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.13)(PORT=1701))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = myserver.mydomain.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.SCHEMA_NAME, "myschema")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertTrue(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void invalidNoHostnameNoUri() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.SCHEMA_NAME, "myschema")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .build());
        assertFalse(connectorConfig.getConfig().validateAndRecord(connectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validBatchDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_DEFAULT), OracleConnectorConfig.DEFAULT_BATCH_SIZE);
        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MAX), OracleConnectorConfig.MAX_BATCH_SIZE);
        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MIN), OracleConnectorConfig.MIN_BATCH_SIZE);

        assertEquals(connectorConfig.getLogMiningBatchSizeDefault(), OracleConnectorConfig.DEFAULT_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMax(), OracleConnectorConfig.MAX_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMin(), OracleConnectorConfig.MIN_BATCH_SIZE);
    }

    @Test
    public void validSleepDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_DEFAULT_MS), OracleConnectorConfig.DEFAULT_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MAX_MS), OracleConnectorConfig.MAX_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MIN_MS), OracleConnectorConfig.MIN_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_INCREMENT_MS), OracleConnectorConfig.SLEEP_TIME_INCREMENT_MS);

        assertEquals(connectorConfig.getLogMiningSleepTimeDefault().toMillis(), OracleConnectorConfig.DEFAULT_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getLogMiningSleepTimeMax().toMillis(), OracleConnectorConfig.MAX_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getLogMiningSleepTimeMin().toMillis(), OracleConnectorConfig.MIN_SLEEP_TIME_MS);
        assertEquals(connectorConfig.getLogMiningSleepTimeIncrement().toMillis(), OracleConnectorConfig.SLEEP_TIME_INCREMENT_MS);
    }

    @Test
    public void validViewFetchSizeDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getConfig().getInteger(OracleConnectorConfig.LOG_MINING_VIEW_FETCH_SIZE), OracleConnectorConfig.DEFAULT_VIEW_FETCH_SIZE);

        assertEquals(connectorConfig.getLogMiningViewFetchSize(), OracleConnectorConfig.DEFAULT_VIEW_FETCH_SIZE);
    }
}
