/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName.LOGMINER;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.buffered.processor.ehcache.CacheCapacityExceededException;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;

/**
 * Specific tests for the Ehcache cache provider implementation.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = LOGMINER)
public class EhcacheIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-8874")
    public void shouldNotSilentlyEvictEventsOverThreshold() throws Exception {
        TestHelper.dropTable(connection, "dbz8874");
        try {
            connection.execute("CREATE TABLE dbz8874 (id numeric(9,0) primary key, data varchar2(4000))");
            TestHelper.streamTable(connection, "dbz8874");

            connection.execute("INSERT INTO dbz8874 (id,data) values (1,'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8874")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, "ehcache")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, TestHelper.getEhcacheGlobalCacheConfig())
                    // Specifically overrides the default with an extremely limited cache size
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, getSmallCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, getSmallCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, getSmallCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, getSmallCacheSize())
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(ErrorHandler.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8874");
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(1);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("snapshot");

            // Add enough data that causes an eviction
            final int EXPECTED_ROWS = 10000;
            final String randomData = RandomStringUtils.randomAlphanumeric(4000);
            for (int i = 0; i < EXPECTED_ROWS; i++) {
                connection.executeWithoutCommitting("INSERT INTO dbz8874 (id,data) values (" + (i + 2) + ",'" + randomData + "')");
            }
            connection.commit();

            // Wait until the connector fails/stops
            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> !isStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME));

            assertThat(logInterceptor.containsThrowableWithCause(CacheCapacityExceededException.class))
                    .as("Expected an Ehcache cache to throw " + CacheCapacityExceededException.class.getName())
                    .isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8874");
        }
    }

    @Test
    @FixFor("DBZ-8874")
    public void shouldNotCauseEvictionWhenCacheSizedProperly() throws Exception {
        TestHelper.dropTable(connection, "dbz8874");
        try {
            connection.execute("CREATE TABLE dbz8874 (id numeric(9,0) primary key, data varchar2(4000))");
            TestHelper.streamTable(connection, "dbz8874");

            connection.execute("INSERT INTO dbz8874 (id,data) values (1,'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8874")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, "ehcache")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, TestHelper.getEhcacheGlobalCacheConfig())
                    // Specifically overrides the default with an extremely limited cache size
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, getLargeCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, getLargeCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, getLargeCacheSize())
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, getLargeCacheSize())
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(ErrorHandler.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8874");
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(1);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("snapshot");

            // Add enough data that causes an eviction
            final int EXPECTED_ROWS = 10000;
            final String randomData = RandomStringUtils.randomAlphanumeric(4000);
            for (int i = 0; i < EXPECTED_ROWS; i++) {
                connection.executeWithoutCommitting("INSERT INTO dbz8874 (id,data) values (" + (i + 2) + ",'" + randomData + "')");
            }
            connection.commit();

            for (int i = 0; i < EXPECTED_ROWS; i++) {
                SourceRecord record = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8874").get(0);
                assertThat(getAfter(record).get("ID")).isEqualTo(2 + i);
                assertThat(getAfter(record).get("DATA")).isEqualTo(randomData);
            }

            assertNoRecordsToConsume();

            assertThat(logInterceptor.containsThrowableWithCause(CacheCapacityExceededException.class))
                    .as("Expected an Ehcache cache to throw " + CacheCapacityExceededException.class.getName())
                    .isFalse();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8874");
        }
    }

    private Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private String getSmallCacheSize() {
        // Ehcache requires a minimum of 64KB for disk persistence for its entry table; hence using 66KB
        return TestHelper.getEhcacheBasicCacheConfig(1024 * 66);
    }

    private String getLargeCacheSize() {
        return TestHelper.getEhcacheBasicCacheConfig(1024 * 1_000_000); // 1GB
    }
}
