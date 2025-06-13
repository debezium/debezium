/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER;
import static io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.runners.Parameterized.Parameters;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;
import javax.management.MBeanServer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration tests for various LogMiner username include/exclude list scenarios.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = ANY_LOGMINER, reason = "LogMiner specific")
@RunWith(Parameterized.class)
public class UsernameFilterIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null && connection.isConnected()) {
            connection.close();
        }
    }

    @Parameters(name = "{index}: lobEnabled={0}")
    public static Collection<Object[]> lobEnabled() {
        return Arrays.asList(new Object[][]{
                { "false" },
                { "true" }
        });
    }

    private final String lobEnabled;

    public UsernameFilterIT(String lobEnabled) {
        this.lobEnabled = lobEnabled;
    }

    @Test
    @FixFor("DBZ-3978")
    public void shouldExcludeEventsByUsernameFilter() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz3978");

            connection.execute("CREATE TABLE dbz3978 (id number(9,0), data varchar2(50), primary key (id))");
            TestHelper.streamTable(connection, "dbz3978");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3978")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_EXCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                    // This test expects the filtering to occur in the connector, not the query
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "none")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO debezium.dbz3978 VALUES (1, 'Test1')");
            connection.executeWithoutCommitting("INSERT INTO debezium.dbz3978 VALUES (2, 'Test2')");
            connection.execute("COMMIT");

            // all messages are filtered out
            assertThat(waitForAvailableRecords(10, TimeUnit.SECONDS)).isFalse();

            Long totalDmlCount = getStreamingMetric("TotalCapturedDmlCount");
            assertThat(totalDmlCount).isGreaterThanOrEqualTo(0L);

        }
        finally {
            TestHelper.dropTable(connection, "dbz3978");
        }
    }

    @Test
    @FixFor("DBZ-9000")
    @SkipWhenAdapterNameIsNot(value = LOGMINER_BUFFERED, reason = "Buffered filters at commit time while unbuffered filters at transaction start")
    public void shouldExcludeEventsByUsernameFilterOnlyAtCommitTime() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz9000");

            connection.execute("CREATE TABLE dbz9000 (id number(9,0), data varchar2(50), primary key (id))");
            TestHelper.streamTable(connection, "dbz9000");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9000")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_EXCLUDE_LIST, "DEBEZIUM")
                    // This test expects the filtering to occur in the connector, not the query
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "none")
                    // Uses legacy behavior
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_MEMORY_LEGACY_TRANSACTION_START, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO debezium.dbz9000 VALUES (1, 'Test1')");
            connection.executeWithoutCommitting("INSERT INTO debezium.dbz9000 VALUES (2, 'Test2')");
            connection.execute("COMMIT");

            // all messages are filtered out
            assertThat(waitForAvailableRecords(10, TimeUnit.SECONDS)).isFalse();

            long expectedTotalDmlCount = 0L;
            if (LogMiningBufferType.MEMORY.equals(TestHelper.getLogMiningBufferType(config))) {
                // Because we've enabled the legacy transaction start behavior for memory caches
                expectedTotalDmlCount = 2L;
            }

            Long totalDmlCount = getStreamingMetric("TotalCapturedDmlCount");
            assertThat(totalDmlCount).isGreaterThanOrEqualTo(expectedTotalDmlCount);

        }
        finally {
            TestHelper.dropTable(connection, "dbz9000");
        }
    }

    @Test
    @FixFor("DBZ-8884")
    public void shouldIncludeEventsByUsernameFilterTransactionSplitOverMultipleMiningSessionsNoQueryFilter() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz8884");
            TestHelper.dropTable(connection, "dbz8884b");

            connection.execute("CREATE TABLE dbz8884 (id number(9,0), data varchar2(50), primary key(id))");
            connection.execute("CREATE TABLE dbz8884b (id number(9,0), data varchar2(50), primary key(id))");

            TestHelper.streamTable(connection, "dbz8884");
            TestHelper.streamTable(connection, "dbz8884b");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8884")
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_INCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                    // This test expects the filtering to occur in the connector, not the query
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "none")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // For this use case, its important that we have some changes that happen in the mining step
            // with the START event for tables that are not collected. This means that the first DML is
            // not matched in the same mining step as the START event.
            for (int i = 1; i <= 10; i++) {
                connection.executeWithoutCommitting(String.format(
                        "INSERT INTO dbz8884b (id,data) values (%d,'Test%d')", i, i));
                Thread.sleep(1000);
            }

            // Now add events for the captured table.
            for (int i = 1; i <= 10; i++) {
                connection.executeWithoutCommitting(String.format(
                        "INSERT INTO dbz8884 (id,data) values (%d,'Test%d')", i, i));
                Thread.sleep(1000);
            }

            connection.execute("COMMIT");

            SourceRecords sourceRecords = consumeRecordsByTopic(10);

            List<SourceRecord> records = sourceRecords.recordsForTopic(topicName("DEBEZIUM", "DBZ8884"));
            assertThat(records).hasSize(10);

            for (int i = 1; i <= 10; i++) {
                SourceRecord record = records.get(i - 1);
                assertThat(getAfter(record).get("ID")).isEqualTo(i);
                assertThat(getAfter(record).get("DATA")).isEqualTo(String.format("Test%d", i));
                assertThat(getSource(record).get("user_name")).isEqualTo("DEBEZIUM");
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8884");
        }
    }

    @Test
    @FixFor("DBZ-8884")
    public void shouldIncludeEventsByUsernameFilterTransactionSplitOverMultipleMiningSessionsWithQueryFilter() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz8884");
            TestHelper.dropTable(connection, "dbz8884b");

            connection.execute("CREATE TABLE dbz8884 (id number(9,0), data varchar2(50), primary key(id))");
            connection.execute("CREATE TABLE dbz8884b (id number(9,0), data varchar2(50), primary key(id))");

            TestHelper.streamTable(connection, "dbz8884");
            TestHelper.streamTable(connection, "dbz8884b");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8884")
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_INCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                    // This test expects the filtering to occur at the query level
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "in")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // For this use case, its important that we have some changes that happen in the mining step
            // with the START event for tables that are not collected. This means that the first DML is
            // not matched in the same mining step as the START event.
            for (int i = 1; i <= 10; i++) {
                connection.executeWithoutCommitting(String.format(
                        "INSERT INTO dbz8884b (id,data) values (%d,'Test%d')", i, i));
                Thread.sleep(1000);
            }

            // Now add events for the captured table.
            for (int i = 1; i <= 10; i++) {
                connection.executeWithoutCommitting(String.format(
                        "INSERT INTO dbz8884 (id,data) values (%d,'Test%d')", i, i));
                Thread.sleep(1000);
            }

            connection.execute("COMMIT");

            SourceRecords sourceRecords = consumeRecordsByTopic(10);

            List<SourceRecord> records = sourceRecords.recordsForTopic(topicName("DEBEZIUM", "DBZ8884"));
            assertThat(records).hasSize(10);

            for (int i = 1; i <= 10; i++) {
                SourceRecord record = records.get(i - 1);
                assertThat(getAfter(record).get("ID")).isEqualTo(i);
                assertThat(getAfter(record).get("DATA")).isEqualTo(String.format("Test%d", i));
                assertThat(getSource(record).get("user_name")).isEqualTo("DEBEZIUM");
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8884");
        }
    }

    @Test
    @FixFor("DBZ-8884")
    public void shouldOnlyCaptureEventsForIncludedUsernames() throws Exception {
        TestHelper.dropTable(connection, "dbz8884");
        try {
            connection.execute("CREATE TABLE dbz8884 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8884");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8884")
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_INCLUDE_LIST, "abc")
                    .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final LogInterceptor logInterceptor = TestHelper.getAbstractEventProcessorLogInterceptor();

            connection.execute("INSERT INTO dbz8884 (id,data) values (1,'abc')");

            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> logInterceptor.containsMessage("Skipped transaction with username DEBEZIUM"));

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8884");
        }
    }

    @Test
    @FixFor("DBZ-8884")
    public void shouldThrowConfigurationExceptionWhenUsernameIncludeExcludeBothSpecified() throws Exception {
        TestHelper.dropTable(connection, "dbz8884");
        try {
            connection.execute("CREATE TABLE dbz8884 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8884");

            LogInterceptor logInterceptor = new LogInterceptor(UsernameFilterIT.class);

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8884")
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_INCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.LOG_MINING_USERNAME_EXCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                    .build();

            start(OracleConnector.class, config);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> logInterceptor.containsErrorMessage("Connector configuration is not valid. The " +
                            "'log.mining.username.exclude.list' value is invalid: \"log.mining.username.include.list\" is already specified"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8884");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getStreamingMetric(String metricName) throws JMException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        return (T) server.getAttribute(
                getStreamingMetricsObjectName(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME),
                metricName);
    }

    private static String topicName(String schemaName, String tableName) {
        return String.format("%s.%s.%s", TestHelper.SERVER_NAME, schemaName, tableName);
    }

    private static Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static Struct getSource(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
    }
}
