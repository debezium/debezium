/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_TYPE;
import static io.debezium.connector.oracle.OracleConnectorConfig.validateEhCacheGlobalConfigField;
import static io.debezium.connector.oracle.OracleConnectorConfig.validateEhcacheConfigFieldRequired;
import static io.debezium.connector.oracle.OracleConnectorConfig.validateLogMiningInfinispanCacheConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

public class OracleConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorConfigTest.class);

    @Test
    public void validXtreamNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerNoUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.HOSTNAME, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validXtreamWithUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.URL, "jdbc:oci:thin:@myserver/mydatabase")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "myserver")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validLogminerWithUrl() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.URL, "MyHostname")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validUrlTNS() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.URL,
                                "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.11)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.12)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=192.68.1.13)(PORT=1701))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = myserver.mydomain.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void invalidNoHostnameNoUri() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .with(OracleConnectorConfig.USER, "debezium")
                        .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                        .with(KafkaSchemaHistory.TOPIC, "history")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void validBatchDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());

        assertEquals(connectorConfig.getLogMiningBatchSizeDefault(), OracleConnectorConfig.DEFAULT_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMax(), OracleConnectorConfig.MAX_BATCH_SIZE);
        assertEquals(connectorConfig.getLogMiningBatchSizeMin(), OracleConnectorConfig.MIN_BATCH_SIZE);
    }

    @Test
    public void validSleepDefaults() throws Exception {

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());

        assertEquals(connectorConfig.getLogMiningSleepTimeDefault(), OracleConnectorConfig.DEFAULT_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeMax(), OracleConnectorConfig.MAX_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeMin(), OracleConnectorConfig.MIN_SLEEP_TIME);
        assertEquals(connectorConfig.getLogMiningSleepTimeIncrement(), OracleConnectorConfig.SLEEP_TIME_INCREMENT);
    }

    @Test
    @FixFor("DBZ-5146")
    public void validQueryFetchSizeDefaults() throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 10_000);
    }

    @Test
    @FixFor("DBZ-5146")
    public void validQueryFetchSizeAvailable() throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(OracleConnectorConfig.QUERY_FETCH_SIZE, 15_000)
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 15_000);
    }

    @Test
    @FixFor("DBZ-2754")
    public void validTransactionRetentionDefaults() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ZERO);
    }

    @Test
    @FixFor("DBZ-2754")
    public void testTransactionRetention() throws Exception {
        final Field transactionRetentionField = OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS;

        Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(transactionRetentionField, 10_800_000)
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ofHours(3));

        config = Configuration.create().with(transactionRetentionField, 0).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        config = Configuration.create().with(transactionRetentionField, -1).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isFalse();
    }

    @Test
    @FixFor("DBZ-6355")
    public void testTransactionRetentionMs() throws Exception {
        final Field transactionRetentionField = OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS;

        Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(transactionRetentionField, 10800000L)
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ofHours(3));

        config = Configuration.create().with(transactionRetentionField, 0).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isTrue();

        config = Configuration.create().with(transactionRetentionField, -1).build();
        assertThat(config.validateAndRecord(Collections.singletonList(transactionRetentionField), LOGGER::error)).isFalse();

        config = Configuration.create().with(transactionRetentionField, 900000L).build();
        connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getLogMiningTransactionRetention()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    @FixFor("DBZ-3557")
    public void testSnapshotLockMode() throws Exception {
        final Field snapshotLockMode = OracleConnectorConfig.SNAPSHOT_LOCKING_MODE;

        Configuration config = Configuration.create().with(snapshotLockMode, "shared")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .build();
        assertThat(config.validateAndRecord(Collections.singletonList(snapshotLockMode), LOGGER::error)).isTrue();

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getSnapshotLockingMode().get().usesLocking()).isTrue();

        config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(snapshotLockMode, "none")
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(snapshotLockMode), LOGGER::error)).isTrue();

        connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getSnapshotLockingMode().get().usesLocking()).isFalse();
    }

    @Test
    @FixFor("DBZ-3813")
    public void testDatabasePortAndRacNodeConfigurations() throws Exception {
        final Field racNodes = OracleConnectorConfig.RAC_NODES;
        final Field port = OracleConnectorConfig.PORT;

        // Test backward compatibility of rac.nodes using no port with database.port
        Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(port, "1521")
                .with(racNodes, "1.2.3.4,1.2.3.5")
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isTrue();

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getRacNodes()).hasSize(2);
        assertThat(connectorConfig.getRacNodes()).contains("1.2.3.4:1521", "1.2.3.5:1521");

        // Test rac.nodes using combination of with/without port with database.port
        config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(port, "1521")
                .with(racNodes, "1.2.3.4,1.2.3.5:1522")
                .build();
        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isTrue();

        connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getRacNodes()).hasSize(2);
        assertThat(connectorConfig.getRacNodes()).contains("1.2.3.4:1521", "1.2.3.5:1522");

        // Test rac.nodes using different ports with no database.port
        config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(racNodes, "1.2.3.4:1523,1.2.3.5:1522")
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isTrue();

        connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getRacNodes()).hasSize(2);
        assertThat(connectorConfig.getRacNodes()).contains("1.2.3.4:1523", "1.2.3.5:1522");

        // Test rac.nodes using different ports that differ from database.port
        config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(port, "1521")
                .with(racNodes, "1.2.3.4:1523,1.2.3.5:1522")
                .build();

        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isTrue();

        connectorConfig = new OracleConnectorConfig(config);
        assertThat(connectorConfig.getRacNodes()).hasSize(2);
        assertThat(connectorConfig.getRacNodes()).contains("1.2.3.4:1523", "1.2.3.5:1522");

        // Test rac.nodes using no port with no database port
        config = Configuration.create().with(racNodes, "1.2.3.4,1.2.3.5").build();
        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isFalse();

        // Test rac.nodes using combination of with/without port and no database.port
        config = Configuration.create().with(racNodes, "1.2.3.4,1.2.3.5:1522").build();
        assertThat(config.validateAndRecord(Collections.singletonList(racNodes), LOGGER::error)).isFalse();
    }

    @Test
    @FixFor("DBZ-2543")
    public void testOpenLogReplicatorConfigFailureWhenNotProvidingRequiredOptions() throws Exception {
        List<Field> fields = List.of(OracleConnectorConfig.OLR_SOURCE, OracleConnectorConfig.OLR_HOST, OracleConnectorConfig.OLR_PORT);
        Configuration config = Configuration.create().with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr").build();
        assertThat(config.validateAndRecord(fields, LOGGER::error)).isFalse();
    }

    @Test
    @FixFor("DBZ-2543")
    public void testOpenLogReplicatorConfig() throws Exception {
        List<Field> fields = List.of(OracleConnectorConfig.OLR_SOURCE, OracleConnectorConfig.OLR_HOST, OracleConnectorConfig.OLR_PORT);
        Configuration config = Configuration.create()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                .with(OracleConnectorConfig.OLR_SOURCE, "oracle")
                .with(OracleConnectorConfig.OLR_HOST, "localhost")
                .with(OracleConnectorConfig.OLR_PORT, "9000")
                .build();
        assertThat(config.validateAndRecord(fields, LOGGER::error)).isTrue();
    }

    @Test
    @FixFor("DBZ-8886")
    public void shouldReturnDefaultValueForInvalidLogMiningBufferWhenValidateLogMiningInfinispanCache() {
        Configuration configuration = Configuration.create()
                .with(LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS.name(), "aValue")
                .with(LOG_MINING_BUFFER_TYPE.name(), "null")
                .build();

        Field.ValidationOutput unreachableState = (field, value, problemMessage) -> {
            throw new RuntimeException("unreachable state");
        };

        List<Integer> actual = Stream.of(
                () -> validateEhCacheGlobalConfigField(configuration, LOG_MINING_ARCHIVE_LOG_ONLY_MODE, unreachableState),
                () -> validateLogMiningInfinispanCacheConfiguration(configuration, LOG_MINING_ARCHIVE_LOG_ONLY_MODE, unreachableState),
                (Supplier<Integer>) () -> validateEhcacheConfigFieldRequired(configuration, LOG_MINING_ARCHIVE_LOG_ONLY_MODE, unreachableState))
                .map(Supplier::get)
                .toList();

        assertThat(actual).containsOnly(0);
    }

    @Test
    @FixFor("DBZ-8886")
    public void shouldReturnInvalidValueForLogMiningBufferWhenValidateLogMiningBufferType() {
        Configuration configuration = Configuration.create()
                .with(LOG_MINING_BUFFER_TYPE.name(), "null")
                .build();

        boolean actual = LOG_MINING_BUFFER_TYPE.validate(
                configuration,
                (field, value, problemMessage) -> assertThat(problemMessage).isEqualTo("Value must be one of ehcache, memory, infinispan_embedded, infinispan_remote"));

        assertThat(actual).isFalse();
    }

    @Test
    @FixFor("DBZ-8886")
    public void shouldReturnValidValueForLogMiningBufferWhenValidateLogMiningBufferType() {
        final Configuration configuration = Configuration.create()
                .with(LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS.name(), "A_CORRECT_VALUE")
                .with(LOG_MINING_BUFFER_TYPE.name(), "infinispan_embedded")
                .build();

        boolean actual = LOG_MINING_BUFFER_TYPE.validate(
                configuration,
                (field, value, problemMessage) -> {
                    throw new RuntimeException("unreachable state");
                });

        assertThat(actual).isTrue();
    }
}
