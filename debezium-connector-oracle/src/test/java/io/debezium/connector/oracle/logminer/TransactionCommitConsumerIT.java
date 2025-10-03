/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang3.RandomStringUtils;
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
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
@SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
public class TransactionCommitConsumerIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.dropAllTables();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-6963")
    public void testShouldNotConsolidateEventsWhenTableHasNoLobColumns() throws Exception {
        try {
            connection.execute("CREATE TABLE addresses (ID numeric(9,0) primary key, person_id numeric(9,0))");
            connection.execute("CREATE TABLE email (ID numeric(9,0) primary key, person_id numeric(9,0))");
            connection.execute("CREATE TABLE phone (ID numeric(9,0) primary key, person_id numeric(9,0))");

            // Seed data
            connection.execute("INSERT INTO addresses values (-1,-1)");
            connection.execute("INSERT INTO email values (-1,-1)");
            connection.execute("INSERT INTO phone values (-1,-1)");

            TestHelper.streamTable(connection, "addresses");
            TestHelper.streamTable(connection, "email");
            TestHelper.streamTable(connection, "phone");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.ADDRESSES,DEBEZIUM\\.EMAIL,DEBEZIUM\\.PHONE")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, "schema_only")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Perform several iterations to create a large transaction with specific sequences of changes against
            // tables that will be viewed by the TransactionCommitConsumer as eligible for merging since LOB is
            // enabled but that shouldn't be because the tables have no LOB columns.
            connection.setAutoCommit(false);
            final int ITERATIONS = 25;
            for (int i = 0; i < ITERATIONS; i++) {
                connection.executeWithoutCommitting("INSERT INTO addresses (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + i + " WHERE id = -1");
                connection.executeWithoutCommitting("INSERT INTO email (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + (i + 999) + " WHERE id = " + i);
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + (i + 1000) + " WHERE id = -1");
                connection.executeWithoutCommitting("UPDATE phone SET person_id = " + i + " WHERE id = -1");
                connection.executeWithoutCommitting("INSERT INTO phone (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE phone SET person_id = " + i + " WHERE id = " + i);
                connection.executeWithoutCommitting("UPDATE phone SET person_id = -1 WHERE id = -1");
                connection.executeWithoutCommitting("UPDATE addresses SET person_id = " + i + " WHERE id = -1");
            }
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(ITERATIONS * 10);

            final List<SourceRecord> addresses = records.recordsForTopic("server1.DEBEZIUM.ADDRESSES");
            assertThat(addresses).hasSize(2 * ITERATIONS);

            for (int i = 0, k = 0; i < addresses.size(); i += 2, k++) {
                VerifyRecord.isValidInsert(addresses.get(i), "ID", k);
                VerifyRecord.isValidUpdate(addresses.get(i + 1), "ID", -1);
            }

            final List<SourceRecord> phones = records.recordsForTopic("server1.DEBEZIUM.PHONE");
            assertThat(phones).hasSize(4 * ITERATIONS);

            for (int i = 0, k = 0; i < phones.size(); i += 4, k++) {
                VerifyRecord.isValidUpdate(phones.get(i), "ID", -1);
                VerifyRecord.isValidInsert(phones.get(i + 1), "ID", k);
                VerifyRecord.isValidUpdate(phones.get(i + 2), "ID", k);
                VerifyRecord.isValidUpdate(phones.get(i + 3), "ID", -1);
            }

            final List<SourceRecord> emails = records.recordsForTopic("server1.DEBEZIUM.EMAIL");
            assertThat(emails).hasSize(4 * ITERATIONS);

            for (int i = 0, k = 0; i < emails.size(); i += 4, k++) {
                VerifyRecord.isValidUpdate(emails.get(i), "ID", -1);
                VerifyRecord.isValidInsert(emails.get(i + 1), "ID", k);
                VerifyRecord.isValidUpdate(emails.get(i + 2), "ID", k);
                VerifyRecord.isValidUpdate(emails.get(i + 3), "ID", -1);
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "phone");
            TestHelper.dropTable(connection, "email");
            TestHelper.dropTable(connection, "addresses");
        }
    }

    @Test
    @FixFor("DBZ-9521")
    public void shouldFlushQueuedLobChangesWhenChangeForTableWithoutLobColumnsAppearsInStream() throws Exception {
        TestHelper.dropTable(connection, "dbz9521a");
        TestHelper.dropTable(connection, "dbz9521b");
        try {
            connection.execute("CREATE TABLE dbz9521a (id numeric(9,0) primary key, data clob, data2 varchar2(50))");
            connection.execute("CREATE TABLE dbz9521b (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9521a");
            TestHelper.streamTable(connection, "dbz9521b");

            // Configure with guardrail limit of 10 tables
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9521A,DEBEZIUM\\.DBZ9521B")
                    .build();

            // The connector should start successfully
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz9521b values (1, 'test1')");

            final Clob clob1 = connection.connection().createClob();
            clob1.setString(1, RandomStringUtils.randomAlphanumeric(8 * 1024));

            connection.prepareUpdate("INSERT INTO dbz9521a values (1, ?, NULL)", ps -> ps.setClob(1, clob1));

            connection.executeWithoutCommitting("INSERT INTO dbz9521b values (2, 'test2')");

            connection.commit();

            // This makes sure that when the second instance of DBZ9521B is seen and its immediately dispatched,
            // the connector automatically flushes any pending events from prior LOB-enabled tables, should any
            // exist before immediately dispatching the non-LOB table.
            final SourceRecords sourceRecords = consumeRecordsByTopic(3);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ9521A")).hasSize(1);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ9521B")).hasSize(2);

            assertThat(sourceRecords.allRecordsInOrder().stream().map(SourceRecord::topic).toList()).containsExactly(
                    "server1.DEBEZIUM.DBZ9521B", "server1.DEBEZIUM.DBZ9521B", "server1.DEBEZIUM.DBZ9521A");
        }
        finally {
            TestHelper.dropTable(connection, "dbz9521a");
            TestHelper.dropTable(connection, "dbz9521b");
        }
    }

    @Test
    @FixFor("DBZ-9521")
    public void shouldNotSkipEventsWhenOnlyNonLobColumnsAreMutatedOnTableWithLobColumns() throws Exception {
        TestHelper.dropTable(connection, "dbz9521a");
        TestHelper.dropTable(connection, "dbz9521b");
        try {
            connection.execute("CREATE TABLE dbz9521a (id numeric(9, 0) primary key, data1 clob, data2 varchar2(50))");
            connection.execute("CREATE TABLE dbz9521b (id numeric(9,0) primary key, data1 clob)");
            TestHelper.streamTable(connection, "dbz9521a");
            TestHelper.streamTable(connection, "dbz9521b");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9521A,DEBEZIUM\\.DBZ9521B")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, "no_data")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Seed data first
            connection.execute("INSERT INTO dbz9521a (id, data1) values (1, 'test')");
            connection.execute("INSERT INTO dbz9521b (id, data1) values (1, 'test')");

            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9521A")).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9521B")).hasSize(1);

            final LogInterceptor interceptor = TestHelper.getEventProcessorLogInterceptor();
            connection.executeWithoutCommitting("INSERT INTO dbz9521a (id, data1) values (2, 'insert')");
            connection.executeWithoutCommitting("INSERT INTO dbz9521b (id, data1) values (2, 'insert')");
            connection.executeWithoutCommitting("UPDATE dbz9521b SET data1 = 'updated' WHERE id = 1");
            connection.executeWithoutCommitting("UPDATE dbz9521a SET data1 = 'updated' WHERE id = 1");
            connection.executeWithoutCommitting("UPDATE dbz9521a SET data2 = 'ok' WHERE id = 1");
            connection.commit();

            // Wait until we mine past the current SCN
            final Scn currentScn = TestHelper.getCurrentScn();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .until(() -> {
                        final BigInteger offsetScn = getStreamingMetric("OffsetScn");
                        return offsetScn != null && Scn.valueOf(offsetScn.toString()).compareTo(currentScn) > 0;
                    });

            assertThat(interceptor.containsMessage("Skipping event "))
                    .as("Should not skip events")
                    .isFalse();

            records = consumeRecordsByTopic(5);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9521A")).hasSize(3);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9521B")).hasSize(2);
        }
        finally {
            TestHelper.dropTable(connection, "dbz9521a");
            TestHelper.dropTable(connection, "dbz9521b");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getStreamingMetric(String metricName) throws JMException {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        final ObjectName objectName = getStreamingMetricsObjectName(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        return (T) mbeanServer.getAttribute(objectName, metricName);
    }

}
