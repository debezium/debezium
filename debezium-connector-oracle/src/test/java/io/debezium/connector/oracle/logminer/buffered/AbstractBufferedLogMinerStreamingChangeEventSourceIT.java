/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * An abstract class for integration tests for {@link BufferedLogMinerStreamingChangeEventSource}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBufferedLogMinerStreamingChangeEventSourceIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropTable(connection, "dbz3752");

        connection.execute("CREATE TABLE dbz3752(id number(9,0) primary key, name varchar2(50))");
        TestHelper.streamTable(connection, "dbz3752");
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz3752");
            connection.close();
        }
    }

    protected abstract Configuration.Builder getBufferImplementationConfig();

    protected boolean hasPersistedState() {
        return false;
    }

    @Test
    @FixFor("DBZ-3752")
    public void shouldResumeFromPersistedState() throws Exception {
        if (!hasPersistedState()) {
            return;
        }

        // Start the connector using the specified buffer & not to drop the buffer across restarts.
        // The testing framework automatically specifies this as true so we need to override it.
        Configuration config = getBufferImplementationConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, false)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3752")
                .build();

        // Start connector and wait for streaming to begin
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO dbz3752 (id,name) values (1, 'Mickey Mouse')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);

        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Mickey Mouse");

        // Stop the connector
        stopConnector();

        connection.execute("INSERT INTO dbz3752 (id,name) values (2, 'Donald Duck')");

        // Restart the connector
        // Upon restart it should rehydrate and begin processing from where it left off.
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO dbz3752 (id,name) values (3, 'Roger Rabbit')");

        records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);

        tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(2);

        after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Donald Duck");

        after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Roger Rabbit");
    }

    @Test
    @FixFor("DBZ-3752")
    public void shouldResumeLongRunningTransactionFromPersistedState() throws Exception {
        if (!hasPersistedState()) {
            return;
        }

        // Start the connector using the specified buffer & not to drop the buffer across restarts.
        // The testing framework automatically specifies this as true so we need to override it.
        Configuration config = getBufferImplementationConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, false)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3752")
                .build();

        // Start connector and wait for streaming to begin
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert two records in two connection, one with a commit and one without.
        try (OracleConnection secondary = TestHelper.testConnection()) {
            connection.executeWithoutCommitting("INSERT INTO dbz3752 (id,name) values (1, 'Mickey Mouse')");
            secondary.execute("INSERT INTO dbz3752 (id,name) values (2, 'Donald Duck')");
        }

        // Get only record
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(1);

        // Assert record state
        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Donald Duck");

        // There should be no more records to consume.
        // The persisted state should contain the Mickey Mouse insert
        assertNoRecordsToConsume();

        // Shutdown the connector
        stopConnector();

        // todo: Verify that (id,name) of (1, 'Mickey Mouse') exists in the persisted data store

        // Add another record while connector off-line
        connection.executeWithoutCommitting("INSERT INTO dbz3752 (id,name) values (3, 'Minnie Mouse')");

        // Restart the connector
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Add last record after restarting
        connection.execute("INSERT INTO dbz3752 (id,name) values (4, 'Roger Rabbit')");

        // Get records
        records = consumeRecordsByTopic(3);
        assertThat(records.allRecordsInOrder()).hasSize(3);
        tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(3);

        after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Mickey Mouse");

        after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Minnie Mouse");

        after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(4);
        assertThat(after.get("NAME")).isEqualTo("Roger Rabbit");
    }

    @Test
    @FixFor("DBZ-8044")
    public void shouldLogAdditionalDetailsForAbandonedTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz8044");
        try {
            connection.execute("CREATE TABLE dbz8044 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8044");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8044")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, "20000")
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz8044 (id,data) values (1, 'test')");

            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .until(() -> logInterceptor.containsMessage(" is being abandoned"));

            connection.commit();

            assertThat(logInterceptor.containsMessage(String.format(", 1 tables [%s.DEBEZIUM.DBZ8044]", TestHelper.getDatabaseName()))).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8044");
        }
    }
}
