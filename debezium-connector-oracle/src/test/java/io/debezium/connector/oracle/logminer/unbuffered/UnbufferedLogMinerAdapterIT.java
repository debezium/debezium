/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * Tests specific to the Oracle LogMiner unbuffered adapter implementation.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
public class UnbufferedLogMinerAdapterIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void beforeEach() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();

        connection = TestHelper.testConnection();
    }

    @AfterEach
    void afterEach() throws SQLException {
        stopConnector();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-9013")
    public void shouldAdvanceOffsetLowWatermarkWhenWhenNoInProgressTransactionsExist() throws Exception {
        TestHelper.dropTable(connection, "dbz9013");
        try {
            connection.execute("CREATE TABLE dbz9013 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9013");

            final LogInterceptor resumePositionLogInterceptor = new LogInterceptor(ResumePositionProvider.class);
            resumePositionLogInterceptor.setLoggerLevel(ResumePositionProvider.class, Level.DEBUG);

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(UnbufferedLogMinerStreamingChangeEventSource.class);
            sourceLogInterceptor.setLoggerLevel(UnbufferedLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9013")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Need to wait for the resume position provider to run once
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> resumePositionLogInterceptor.containsMessage("Resume/Commit SCN "));

            // Insert a new transaction, which carries the new offset details
            connection.execute("INSERT INTO dbz9013 (id,data) values (1,'test')");

            // Wait until the low watermark is advanced
            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> sourceLogInterceptor.containsMessage("Advancing offset low-watermark scn"));

            // Make sure the event is received before stopping the connector
            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9013")).hasSize(1);

            stopConnector();

            // Read the offsets
            final OraclePartition partition = new OraclePartition(TestHelper.SERVER_NAME, TestHelper.DATABASE);
            final Map<String, Object> committedOffsets = readLastCommittedOffset(config, partition.getSourcePartition());

            // Get SCN passed from the snapshot into the streaming phase
            final Scn snapshotScn = OracleOffsetContext.loadSnapshotScn(committedOffsets);
            assertThat(snapshotScn).isNotNull();

            // Get the SCN low watermark updated by the streaming phase
            final Scn lowWatermarkScn = OracleOffsetContext.getScnFromOffsetMapByKey(committedOffsets, SourceInfo.SCN_KEY);
            assertThat(lowWatermarkScn).isNotNull();

            // Verify the SCN values were updated
            // Before this fix, the lowWatermarkScn was always the same as the snapshotScn
            // With the fix, the low watermark should be after the snapshot scn
            assertThat(lowWatermarkScn.asBigInteger()).isGreaterThan(snapshotScn.asBigInteger());
        }
        finally {
            TestHelper.dropTable(connection, "dbz9013");
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldNotEmitRollbackFlaggedEventsWhenRollingBackToSavepoint() throws Exception {
        TestHelper.dropTable(connection, "dbz1960");
        try {
            connection.execute("CREATE TABLE dbz1960 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz1960");

            // Every DML row is logged at DEBUG with its ROLLBACK column rendered as rollbackFlag=<bool>
            final LogInterceptor sourceLogInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            sourceLogInterceptor.setLoggerLevel(AbstractLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1960")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz1960 (id,data) values (1,'insert')");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1960")).hasSize(1);
            VerifyRecord.isValidInsert(records.recordsForTopic("server1.DEBEZIUM.DBZ1960").get(0), "ID", 1);

            // Two changes that survive, then a savepoint covering one of each DML type that is rolled back
            connection.execute("BEGIN " +
                    "UPDATE dbz1960 SET data = 'update' WHERE id = 1;" +
                    "INSERT INTO dbz1960 (id,data) values (2,'insert');" +
                    "SAVEPOINT a;" +
                    "UPDATE dbz1960 SET data = 'rolled-back' WHERE id = 1;" +
                    "INSERT INTO dbz1960 (id,data) values (3,'rolled-back');" +
                    "DELETE FROM dbz1960 WHERE id = 2;" +
                    "ROLLBACK TO SAVEPOINT a;" +
                    "COMMIT;" +
                    "END;");

            records = consumeRecordsByTopic(2);
            final List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ1960");
            assertThat(tableRecords).hasSize(2);

            VerifyRecord.isValidUpdate(tableRecords.get(0), "ID", 1);
            assertThat(getAfter(tableRecords.get(0)).get("DATA")).isEqualTo("update");

            VerifyRecord.isValidInsert(tableRecords.get(1), "ID", 2);
            assertThat(getAfter(tableRecords.get(1)).get("DATA")).isEqualTo("insert");

            assertNoRecordsToConsume();

            stopConnector();

            // In COMMITTED_DATA_ONLY mode LogMiner withholds the three rolled-back statements entirely,
            // neither their forward rows nor their ROLLBACK=1 undo rows are surfaced, so the unbuffered
            // implementation never observes a partial rollback for a savepoint. Pin that here so a change
            // in LogMiner's behaviour shows up as a failed count rather than as spurious change events.
            final List<String> dmlRows = sourceLogInterceptor.getLogEntriesThatContainsMessage("DML: ");
            assertThat(dmlRows).as("observed DML rows: %s", dmlRows)
                    .filteredOn(row -> row.contains("rollbackFlag=true")).isEmpty();
            assertThat(dmlRows).as("observed DML rows: %s", dmlRows)
                    .filteredOn(row -> row.contains("rollbackFlag=false")).hasSize(3);
        }
        finally {
            TestHelper.dropTable(connection, "dbz1960");
        }
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void shouldNotEmitRollbackFlaggedEventsOnConstraintViolation() throws Exception {
        TestHelper.dropTable(connection, "dbz1960");
        try {
            connection.execute("CREATE TABLE dbz1960 (id numeric(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz1960 ON dbz1960 (id)");
            TestHelper.streamTable(connection, "dbz1960");

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            sourceLogInterceptor.setLoggerLevel(AbstractLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1960")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // The second insert is written to redo and then undone by a statement-level rollback
            try {
                connection.executeWithoutCommitting("INSERT INTO dbz1960 (id,data) values (1,'insert')");
                connection.executeWithoutCommitting("INSERT INTO dbz1960 (id,data) values (1,'rolled-back')");
            }
            catch (SQLException e) {
                if (!e.getMessage().startsWith("ORA-00001")) {
                    throw e;
                }
            }
            finally {
                connection.executeWithoutCommitting("COMMIT");
            }

            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ1960");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
            assertThat(getAfter(tableRecords.get(0)).get("DATA")).isEqualTo("insert");

            assertNoRecordsToConsume();

            stopConnector();

            final List<String> dmlRows = sourceLogInterceptor.getLogEntriesThatContainsMessage("DML: ");
            assertThat(dmlRows).as("observed DML rows: %s", dmlRows)
                    .filteredOn(row -> row.contains("rollbackFlag=true")).isEmpty();
            assertThat(dmlRows).as("observed DML rows: %s", dmlRows)
                    .filteredOn(row -> row.contains("rollbackFlag=false")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz1960");
        }
    }

    private static Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

}
