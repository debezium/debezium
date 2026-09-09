/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.adb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipWhenNotAutonomous;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration tests that verify the connector resumes streaming from archive logs after a
 * restart when connected to an Oracle Autonomous Database.
 *
 * @author Chris Cranford
 */
@SkipWhenNotAutonomous(reason = "Tests verify streaming resumption from archive logs on an Autonomous Database")
public class AutonomousRestartIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropTable(connection, "dbz1106");
        connection.execute("CREATE TABLE dbz1106 (id numeric(9,0), data varchar2(50), primary key(id))");
        TestHelper.streamTable(connection, "dbz1106");
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz1106");
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#1106")
    public void shouldResumeStreamingFromArchiveLogsAfterRestart() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1106")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO dbz1106 (id, data) values (1, 'before restart')");
        TestHelper.forceStreamingVisibility();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ1106"));
        assertThat(topicRecords).hasSize(1);
        VerifyRecord.isValidInsert(topicRecords.get(0), "ID", 1);

        stopConnector();

        // These changes are only present in the logs when the connector restarts
        connection.execute("INSERT INTO dbz1106 (id, data) values (2, 'while stopped')");
        connection.execute("INSERT INTO dbz1106 (id, data) values (3, 'while stopped')");

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        TestHelper.forceStreamingVisibility();

        records = consumeRecordsByTopic(2);
        topicRecords = records.recordsForTopic(topicName("DBZ1106"));
        assertThat(topicRecords).hasSize(2);

        VerifyRecord.isValidInsert(topicRecords.get(0), "ID", 2);
        VerifyRecord.isValidInsert(topicRecords.get(1), "ID", 3);

        final Struct after = ((Struct) topicRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("DATA")).isEqualTo("while stopped");
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }
}