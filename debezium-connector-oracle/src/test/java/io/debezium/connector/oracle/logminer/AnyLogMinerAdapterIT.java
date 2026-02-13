/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Generic LogMiner-specific tests for any LogMiner adapter.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Requires LogMiner")
public class AnyLogMinerAdapterIT extends AbstractAsyncEngineConnectorTest {

    private static OracleConnection connection;

    @BeforeAll
    static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterAll
    static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void before() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-9636")
    public void shouldTrackRsId() throws Exception {
        TestHelper.dropTable(connection, "dbz9636");
        try {
            connection.execute("CREATE TABLE dbz9636 (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9636");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9636")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9636 (id,data) values (1, 'test')");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ9636");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(source.schema().field(CommitScn.ROLLBACK_SEGMENT_ID_KEY)).isNotNull();
            assertThat(source.get(CommitScn.ROLLBACK_SEGMENT_ID_KEY)).isNotNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9636");
        }
    }

    @Test
    @FixFor("DBZ-9636")
    public void shouldNotTrackRsId() throws Exception {
        TestHelper.dropTable(connection, "dbz9636");
        try {
            connection.execute("CREATE TABLE dbz9636 (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9636");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9636")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9636 (id,data) values (1, 'test')");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ9636");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(source.schema().field(CommitScn.ROLLBACK_SEGMENT_ID_KEY)).isNotNull();
            assertThat(source.get(CommitScn.ROLLBACK_SEGMENT_ID_KEY)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9636");
        }
    }
}
