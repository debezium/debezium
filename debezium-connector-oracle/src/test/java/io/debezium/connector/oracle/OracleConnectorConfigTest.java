/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.history.KafkaDatabaseHistory;

public class OracleConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorConfigTest.class);

    private static final String TABLENAME_CASE_INSENSITIVE_WARNING = "The option '" + OracleConnectorConfig.TABLENAME_CASE_INSENSITIVE
            + "' is deprecated and will be removed in the future.";

    @Test
    public void validXtreamNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
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
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerWithUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.URL, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
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
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void invalidNoHostnameNoUri() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(OracleConnectorConfig.SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaDatabaseHistory.TOPIC, "history")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validBatchDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getLogMiningBatchSizeDefault(), OracleConnectorConfig.DEFAULT_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMax(), OracleConnectorConfig.MAX_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMin(), OracleConnectorConfig.MIN_BATCH_SIZE);
    }

    @Test
    public void validSleepDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getLogMiningSleepTimeDefault(), OracleConnectorConfig.DEFAULT_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeMax(), OracleConnectorConfig.MAX_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeMin(), OracleConnectorConfig.MIN_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeIncrement(), OracleConnectorConfig.SLEEP_TIME_INCREMENT);
    }

    @Test
    public void validViewFetchSizeDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .build());

        assertEquals(connectorConfig.getLogMiningViewFetchSize(), OracleConnectorConfig.DEFAULT_VIEW_FETCH_SIZE);
    }

    @Test
    @FixFor("DBZ-2754")
    public void validTransactionRetentionDefaults() throws Exception {
        final Configuration config = Configuration.create().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ZERO);
    }

    @Test
    @FixFor("DBZ-2754")
    public void testTransactionRetention() throws Exception {
        final Field transactionRetentionField = OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION;

        Configuration config = Configuration.create().with(transactionRetentionField, 3).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ofHours(3));

        config = Configuration.create().with(transactionRetentionField, 0).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        config = Configuration.create().with(transactionRetentionField, -1).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isFalse();
    }

    @Test
    @FixFor("DBZ-3190")
    public void shouldLogDeprecationWarningForTablenameCaseInsensitiveTrue() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create().with(OracleConnectorConfig.TABLENAME_CASE_INSENSITIVE, true).build());

        assertThat(connectorConfig.getTablenameCaseInsensitive()).isEqualTo(Optional.of(true));
        assertThat(logInterceptor.containsMessage(TABLENAME_CASE_INSENSITIVE_WARNING)).isTrue();
    }

    @Test
    @FixFor("DBZ-3190")
    public void shouldLogDeprecationWarningForTablenameCaseInsensitiveFalse() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create().with(OracleConnectorConfig.TABLENAME_CASE_INSENSITIVE, false).build());

        assertThat(connectorConfig.getTablenameCaseInsensitive()).isEqualTo(Optional.of(false));
        assertThat(logInterceptor.containsMessage(TABLENAME_CASE_INSENSITIVE_WARNING)).isTrue();
    }

    @Test
    @FixFor("DBZ-3190")
    public void shouldNotBePresentWhenTablenameCaseInsensitiveNotSupplied() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(Configuration.create().build());
        assertThat(connectorConfig.getTablenameCaseInsensitive()).isEqualTo(Optional.empty());
        assertThat(logInterceptor.containsMessage(TABLENAME_CASE_INSENSITIVE_WARNING)).isFalse();
    }
}
