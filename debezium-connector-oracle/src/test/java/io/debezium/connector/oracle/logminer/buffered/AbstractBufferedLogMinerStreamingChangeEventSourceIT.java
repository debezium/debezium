/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.util.OracleMetricsHelper;
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

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropTable(connection, "dbz3752");

        connection.execute("CREATE TABLE dbz3752(id number(9,0) primary key, name varchar2(50))");
        TestHelper.streamTable(connection, "dbz3752");
    }

    @AfterEach
    void after() throws Exception {
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

    @Test
    @FixFor("DBZ-1553")
    public void shouldAdvanceMiningWindowForLongRunningTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz1553");
        try {
            connection.execute("CREATE TABLE dbz1553 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz1553");

            // Configure the connector with a 30 second window max
            Configuration config = getBufferImplementationConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1553")
                    .with(OracleConnectorConfig.LOG_MINING_WINDOW_MAX_MS, "30000")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start a long-running transaction that will not be committed
            connection.executeWithoutCommitting("INSERT INTO dbz1553 (id,data) values (1, 'long-running')");

            // Wait for the window threshold to be exceeded and the mining window to be advanced.
            // The log message should appear once the mining window lower bound is moved past the
            // long-running transaction.
            Awaitility.await()
                    .atMost(Duration.ofMinutes(2))
                    .pollInterval(Duration.ofSeconds(5))
                    .until(() -> logInterceptor.containsWarnMessage("Mining window lower bound advanced"));

            // Verify the warning message indicates the window was advanced due to the threshold
            assertThat(logInterceptor.containsWarnMessage("due to log.mining.window.max.ms threshold")).isTrue();

            // Now commit the long-running transaction
            connection.commit();

            // Consume the record to verify the transaction was fully captured
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ1553");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("long-running");
        }
        finally {
            TestHelper.dropTable(connection, "dbz1553");
        }
    }

    @Test
    @FixFor("debezium/dbz#1914")
    public void shouldRollbackToSavepointIdempotently() throws Exception {
        TestHelper.dropTable(connection, "dbz1914");
        try {
            connection.execute("CREATE TABLE dbz1914 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz1914");

            Configuration config = getBufferImplementationConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1914")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            OracleMetricsHelper.waitForOffsetScnAfter(Scn.NULL);

            Scn offsetScn = Scn.valueOf(OracleMetricsHelper.getOffsetScn().toString());
            connection.executeWithoutCommitting(
                    "INSERT INTO dbz1914(id,data) VALUES (1,'insert 1')",
                    "SAVEPOINT s1",
                    "UPDATE dbz1914 SET data = 'update 1' WHERE id = 1",
                    "ROLLBACK TO SAVEPOINT s1");
            OracleMetricsHelper.waitForOffsetScnAfter(offsetScn);
            connection.executeWithoutCommitting("INSERT INTO dbz1914 (id,data) VALUES (2,'insert 2')");
            connection.commit();

            List<SourceRecord> tableRecords = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1914");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("insert 1");
        }
        finally {
            TestHelper.dropTable(connection, "dbz1914");
        }
    }

    // Rollback to savepoint - INSERT

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackInsertScalar() throws Exception {
        String tableName = "DBZ1960_01";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR(50))";
        String[] statements = new String[]{
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_01 (ID, STR0) VALUES (1, 'STR0-1-0')",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_01 (ID, STR0) VALUES (2, 'STR0-2-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 1, statements);
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("STR0")).isEqualTo("STR0-2-0");
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackInsertEmpty() throws Exception {
        String tableName = "DBZ1960_02";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_02 (ID, LOB0) VALUES (1, EMPTY_CLOB())",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_02 (ID, LOB0) VALUES (2, 'LOB0-2-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 1, statements);
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-2-0");
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackInsertInline() throws Exception {
        String tableName = "DBZ1960_03";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_03 (ID, LOB0) VALUES (1, 'LOB0-1-0')",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_03 (ID, LOB0) VALUES (2, 'LOB0-2-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 1, statements);
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-2-0");
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackInsertOutOfLine() throws Exception {
        String tableName = "DBZ1960_04";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_04 (ID, XML0, LOB0, EXT0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('LOB0-1-', 1985, '0'), RPAD('EXT0-1-', 4000, '0'))",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_04 (ID, XML0, LOB0, EXT0) VALUES (2, XMLTYPE('<XML0><ID>2</ID><V>0</V></XML0>'), 'LOB0-2-0', 'EXT0-2-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 1, statements);
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>2</ID><V>0</V></XML0>");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-2-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-2-0");
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackInsertInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_05";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, EXT0 VARCHAR2(8000), XML0 XMLTYPE, LOB0 CLOB, LOB1 CLOB)";
        String[] statements = new String[]{
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_05 (ID, EXT0, XML0, LOB0, LOB1) VALUES (1, RPAD('EXT0-1-', 4000, '0'), XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('LOB0-1-', 1985, '0'), 'LOB1-1-0')",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_05 (ID, EXT0, XML0, LOB0, LOB1) VALUES (2, 'EXT0-2-0', XMLTYPE('<XML0><ID>2</ID><V>0</V></XML0>'), 'LOB0-2-0', 'LOB1-2-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 1, statements);
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("EXT0")).isEqualTo("EXT0-2-0");
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>2</ID><V>0</V></XML0>");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-2-0");
        assertThat(after.get("LOB1")).isEqualTo("LOB1-2-0");
    }

    // Rollback to savepoint - UPDATE

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateScalar() throws Exception {
        String tableName = "DBZ1960_06";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR(50))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_06 (ID, STR0) VALUES (1, 'STR0-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_06 SET STR0 = 'STR0-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_06 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateEmpty() throws Exception {
        String tableName = "DBZ1960_07";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_07 (ID, LOB0) VALUES (1, NULL)",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_07 SET LOB0 = EMPTY_CLOB() WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_07 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo(null);
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateInline() throws Exception {
        String tableName = "DBZ1960_08";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_08 (ID, LOB0) VALUES (1, 'LOB0-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_08 SET LOB0 = 'LOB0-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_08 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateOutOfLine() throws Exception {
        String tableName = "DBZ1960_09";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_09 (ID, XML0, LOB0, EXT0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'LOB0-1-0', 'EXT0-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_09 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), LOB0 = RPAD('LOB0-1-', 1985, '1'), EXT0 = RPAD('EXT0-1-', 4000, '1') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_09 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>0</V></XML0>");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateXml() throws Exception {
        String tableName = "DBZ1960_10";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_10 (ID, XML0) VALUES (1, '<XML0><ID>1</ID><V>0</V></XML0>')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_10 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_10 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>0</V></XML0>");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateScalarAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_11";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_11 (ID, STR0, EXT0, LOB0) VALUES (1, 'STR0-1-0', 'EXT0-1-0', 'LOB0-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_11 SET STR0 = 'STR0-1-1', XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), EXT0 = RPAD('EXT0-1-', 4000, '1'), LOB0 = RPAD('LOB0-1-', 1985, '1') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_11 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("XML0")).isEqualTo(null);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateScalarAndXml() throws Exception {
        String tableName = "DBZ1960_12";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), XML0 XMLTYPE)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_12 (ID, STR0, XML0) VALUES (1, 'STR0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'))",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_12 SET STR0 = 'STR0-1-1', XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_12 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>0</V></XML0>");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_13";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_13 (ID, LOB0, XML0, EXT0, LOB1) VALUES (1, 'LOB0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'EXT0-1-0', 'LOB1-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_13 SET LOB0 = RPAD('LOB0-1-', 1985, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), EXT0 = RPAD('EXT0-1-', 4000, '1'), LOB1 = 'LOB1-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_13 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>0</V></XML0>");
        assertThat(after.get("LOB1")).isEqualTo("LOB1-1-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateScalarAndInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_14";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_14 (ID, STR0, LOB0, EXT0, LOB1) VALUES (1, 'STR0-1-0', 'LOB0-1-0', 'EXT0-1-0', 'LOB1-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_14 SET STR0 = 'STR0-1-1', EXT0 = RPAD('EXT0-1-', 4000, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), LOB0 = RPAD('LOB0-1-', 1985, '1'), LOB1 = 'LOB1-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_14 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(after.get("XML0")).isEqualTo(null);
        assertThat(after.get("LOB1")).isEqualTo("LOB1-1-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackLobWrite() throws Exception {
        String tableName = "DBZ1960_15";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_15 (ID, LOB0) VALUES (1, 'LOB0-1-0')",
                "SAVEPOINT s1",
                "DECLARE\n" +
                        "  loc CLOB;\n" +
                        "BEGIN\n" +
                        "  SELECT LOB0 INTO loc FROM DBZ1960_15 WHERE ID = 1 FOR UPDATE;\n" +
                        "  DBMS_LOB.WRITE(loc, 1985, 1, RPAD('LOB0-1-', 1985, '1'));\n" +
                        "END;",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_15(ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackLobTrim() throws Exception {
        String tableName = "DBZ1960_16";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_16 (ID, LOB0) VALUES (1, 'LOB0-1-00')",
                "SAVEPOINT s1",
                "DECLARE\n" +
                        "  loc CLOB;\n" +
                        "BEGIN\n" +
                        "  SELECT LOB0 INTO loc FROM DBZ1960_16 WHERE ID = 1 FOR UPDATE;\n" +
                        "  DBMS_LOB.TRIM(loc, 8);\n" +
                        "END;",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_16(ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, true, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-00");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    // Rollback to savepoint - DELETE

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackDelete() throws Exception {
        String tableName = "DBZ1960_17";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_17 (ID, STR0, LOB0, XML0, EXT0, LOB1) VALUES (1, 'STR0-1-0', RPAD('LOB0-1-', 1985, '0'), XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('EXT0-1-', 4000, '0'), 'LOB1-1-0')",
                "SAVEPOINT s1",
                "DELETE FROM DBZ1960_17 WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_17 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-" + "0".repeat(1978));
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>0</V></XML0>");
        assertThat(after.get("LOB1")).isEqualTo("LOB1-1-0");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-" + "0".repeat(3993));
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    // Rollback to savepoint - Multiple statements

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbacks() throws Exception {
        String tableName = "DBZ1960_18";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_18 (ID, STR0) VALUES (1, 'STR0-1-0')",
                "INSERT INTO DBZ1960_18 (ID, STR0) VALUES (2, 'STR0-2-0')",
                "SAVEPOINT s1",
                "INSERT INTO DBZ1960_18 (ID, STR0) VALUES (3, 'STR0-3-0')",
                "UPDATE DBZ1960_18 SET STR0 = 'STR0-2-1' WHERE ID = 2",
                "DELETE FROM DBZ1960_18 WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_18 (ID, STR0) VALUES (4, 'STR0-4-0')", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 3, statements);
        assertThat(tableRecords).hasSize(3);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("STR0")).isEqualTo("STR0-2-0");
        after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(4);
        assertThat(after.get("STR0")).isEqualTo("STR0-4-0");
    }

    // Rollback to savepoint - Supported without INTERNAL

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackUpdateOutOfLineWithoutInternal() throws Exception {
        String tableName = "DBZ1960_19";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_19 (ID, LOB0) VALUES (1, 'LOB0-1-0')",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_19 SET LOB0 = RPAD('LOB0-1-', 1985, '0') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_19 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldInsertEmptyWithoutInternalAndRollbackUpdateScalar() throws Exception {
        String tableName = "DBZ1960_20";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_20 (ID, STR0, LOB0) VALUES (1, 'STR0-1-0', EMPTY_CLOB())",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_20 SET STR0 = 'STR0-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_20 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("LOB0")).isEqualTo(null);
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldUpdateEmptyWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_21";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, LOB1 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_21 (ID, LOB0, LOB1) VALUES (1, NULL, NULL)",
                "UPDATE DBZ1960_21 SET LOB0 = EMPTY_CLOB(), LOB1 = EMPTY_CLOB() WHERE ID = 1",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_21 SET LOB0 = RPAD('LOB0-1-', 1985, '2'), LOB1 = 'LOB1-1-2' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_21 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo(null);
        assertThat(after.get("LOB1")).isEqualTo(null);
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldUpdateOutOfLineWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_22";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, EXT0 VARCHAR2(8000))";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_22 (ID, LOB0, EXT0) VALUES (1, 'LOB0-1-0', 'EXT0-1-0')",
                "UPDATE DBZ1960_22 SET LOB0 = RPAD('LOB0-1-', 1985, '1') WHERE ID = 1",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_22 SET LOB0 = 'LOB0-1-2', EXT0 = RPAD('EXT0-1-', 4000, '2') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_22 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-" + "1".repeat(1978));
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldInsertOutOfLineWithoutInternalAndRollbackUpdateScalar() throws Exception {
        String tableName = "DBZ1960_23";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_23 (ID, STR0, LOB0) VALUES (1, 'STR0-1-0', RPAD('LOB0-1-', 1985, '0'))",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_23 SET STR0 = 'STR0-1-1' WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_23 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("STR0")).isEqualTo("STR0-1-0");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-" + "0".repeat(1978));
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldUpdateXmlWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        String tableName = "DBZ1960_24";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, EXT0 VARCHAR2(8000), LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_24 (ID, XML0, EXT0, LOB0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'EXT0-1-0', 'LOB0-1-0')",
                "UPDATE DBZ1960_24 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_24 SET EXT0 = 'EXT0-1-2', LOB0 = RPAD('LOB0-1-', 1985, '2') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_24 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>1</V></XML0>");
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-0");
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldUpdateOutOfLineAndXmlWithoutInternalAndRollbackUpdateInlineAndXml() throws Exception {
        String tableName = "DBZ1960_25";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, EXT0 VARCHAR2(8000), XML0 XMLTYPE)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_25 (ID, EXT0, XML0) VALUES (1, 'EXT0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'))",
                "UPDATE DBZ1960_25 SET EXT0 = RPAD('EXT0-1-', 4000, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1",
                "SAVEPOINT s1",
                "UPDATE DBZ1960_25 SET EXT0 = 'EXT0-1-2', XML0 = XMLTYPE('<XML0><ID>1</ID><V>2</V></XML0>') WHERE ID = 1",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_25 (ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("EXT0")).isEqualTo("EXT0-1-" + "1".repeat(3993));
        assertThat(after.get("XML0")).isEqualTo("<XML0><ID>1</ID><V>1</V></XML0>");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackLobWriteWithoutInternal() throws Exception {
        String tableName = "DBZ1960_26";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_26 (ID, LOB0) VALUES (1, 'LOB0-1-0')",
                "SAVEPOINT s1",
                "DECLARE\n" +
                        "  loc CLOB;\n" +
                        "BEGIN\n" +
                        "  SELECT LOB0 INTO loc FROM DBZ1960_26 WHERE ID = 1 FOR UPDATE;\n" +
                        "  DBMS_LOB.WRITE(loc, 1985, 1, RPAD('LOB0-1-', 1985, '1'));\n" +
                        "END;",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_26(ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-0");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldRollbackLobTrimWithoutInternal() throws Exception {
        String tableName = "DBZ1960_27";
        String tableSpec = "(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB)";
        String[] statements = new String[]{
                "INSERT INTO DBZ1960_27 (ID, LOB0) VALUES (1, 'LOB0-1-00')",
                "SAVEPOINT s1",
                "DECLARE\n" +
                        "  loc CLOB;\n" +
                        "BEGIN\n" +
                        "  SELECT LOB0 INTO loc FROM DBZ1960_27 WHERE ID = 1 FOR UPDATE;\n" +
                        "  DBMS_LOB.TRIM(loc, 8);\n" + // DBMS_LOB.ERASE(loc, 1, 8);
                        "END;",
                "ROLLBACK TO SAVEPOINT s1",
                "INSERT INTO DBZ1960_27(ID) VALUES (2)", };
        List<SourceRecord> tableRecords = execute(tableName, tableSpec, false, 2, statements);
        assertThat(tableRecords).hasSize(2);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("LOB0")).isEqualTo("LOB0-1-00");
        assertThat(((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(2);
    }

    private List<SourceRecord> execute(String tableName, String tableSpec, boolean includeInternalEvents, int numRecords, String[] statements)
            throws Exception {
        TestHelper.dropTable(connection, tableName);
        try {
            connection.execute("CREATE TABLE " + tableName + tableSpec);
            TestHelper.streamTable(connection, tableName);

            Configuration config = getBufferImplementationConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\." + tableName)
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.LOG_MINING_INCLUDE_INTERNAL_EVENTS, String.valueOf(includeInternalEvents))
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute(statements);

            return consumeRecordsByTopic(numRecords).recordsForTopic("server1.DEBEZIUM." + tableName);
        }
        finally {
            TestHelper.dropTable(connection, tableName);
        }
    }
}
