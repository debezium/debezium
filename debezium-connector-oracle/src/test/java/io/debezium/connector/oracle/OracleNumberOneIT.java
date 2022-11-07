/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.converters.NumberOneToBooleanConverter;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests the behavior of {@code NUMBER(1)} data types.
 *
 * @author Chris Cranford
 */
public class OracleNumberOneIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.number_one_test");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute("CREATE TABLE NUMBER_ONE_TEST (id numeric primary key, data number(1) default 0, data2 number(1) default 1)");
        TestHelper.streamTable(connection, "debezium.number_one_test");
    }

    @After
    public void after() throws Exception {
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "debezium.number_one_test");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-3208")
    public void shouldHandleNumberOneAsNumber() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.NUMBER_ONE_TEST")
                .build();

        // Insert snapshot data
        insertDataBatchStartingAtKey(1);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Consume snapshot records
        SourceRecords records = consumeRecordsByTopic(5);
        List<SourceRecord> testRecords = records.recordsForTopic("server1.DEBEZIUM.NUMBER_ONE_TEST");
        assertThat(testRecords).hasSize(5);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(1), Byte.valueOf("0"), Byte.valueOf("0"));
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(2), Byte.valueOf("1"), Byte.valueOf("1"));
        assertRecordNumberValue(testRecords, 2, BigDecimal.valueOf(3), Byte.valueOf("2"), Byte.valueOf("2"));
        assertRecordNumberValue(testRecords, 3, BigDecimal.valueOf(4), Byte.valueOf("-1"), Byte.valueOf("-1"));
        assertRecordNumberValue(testRecords, 4, BigDecimal.valueOf(5), Byte.valueOf("0"), Byte.valueOf("1"));

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert stream data
        insertDataBatchStartingAtKey(6);

        // Consume stream records
        records = consumeRecordsByTopic(5);
        testRecords = records.recordsForTopic("server1.DEBEZIUM.NUMBER_ONE_TEST");
        assertThat(testRecords).hasSize(5);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(6), Byte.valueOf("0"), Byte.valueOf("0"));
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(7), Byte.valueOf("1"), Byte.valueOf("1"));
        assertRecordNumberValue(testRecords, 2, BigDecimal.valueOf(8), Byte.valueOf("2"), Byte.valueOf("2"));
        assertRecordNumberValue(testRecords, 3, BigDecimal.valueOf(9), Byte.valueOf("-1"), Byte.valueOf("-1"));
        assertRecordNumberValue(testRecords, 4, BigDecimal.valueOf(10), Byte.valueOf("0"), Byte.valueOf("1"));
    }

    @Test
    @FixFor("DBZ-3208")
    public void shouldHandleNumberOneAsBoolean() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.NUMBER_ONE_TEST")
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "boolean")
                .with("boolean.type", NumberOneToBooleanConverter.class.getName())
                .with("boolean.selector", ".*NUMBER_ONE_TEST\\.DATA.*")
                .build();

        // Insert snapshot data
        insertDataBatchStartingAtKey(1);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Consume snapshot records
        SourceRecords records = consumeRecordsByTopic(5);
        List<SourceRecord> testRecords = records.recordsForTopic("server1.DEBEZIUM.NUMBER_ONE_TEST");
        assertThat(testRecords).hasSize(5);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(1), false, false);
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(2), true, true);
        assertRecordNumberValue(testRecords, 2, BigDecimal.valueOf(3), true, true);
        assertRecordNumberValue(testRecords, 3, BigDecimal.valueOf(4), false, false);
        assertRecordNumberValue(testRecords, 4, BigDecimal.valueOf(5), false, true);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert stream data
        insertDataBatchStartingAtKey(6);

        // Consume stream records
        records = consumeRecordsByTopic(5);
        testRecords = records.recordsForTopic("server1.DEBEZIUM.NUMBER_ONE_TEST");
        assertThat(testRecords).hasSize(5);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(6), false, false);
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(7), true, true);
        assertRecordNumberValue(testRecords, 2, BigDecimal.valueOf(8), true, true);
        assertRecordNumberValue(testRecords, 3, BigDecimal.valueOf(9), false, false);
        assertRecordNumberValue(testRecords, 4, BigDecimal.valueOf(10), false, true);
    }

    private void assertRecordNumberValue(List<SourceRecord> records, int index, Object key, Object data, Object data2) {
        final Struct after = (Struct) ((Struct) records.get(index).value()).get(FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(key);
        assertThat(after.get("DATA")).isEqualTo(data);
        assertThat(after.get("DATA2")).isEqualTo(data2);
    }

    private void insertDataBatchStartingAtKey(int initialKey) throws SQLException {
        connection.executeWithoutCommitting("INSERT INTO debezium.number_one_test values (" + initialKey + ", 0, 0)");
        connection.executeWithoutCommitting("INSERT INTO debezium.number_one_test values (" + (initialKey + 1) + ", 1, 1)");
        connection.executeWithoutCommitting("INSERT INTO debezium.number_one_test values (" + (initialKey + 2) + ", 2, 2)");
        connection.executeWithoutCommitting("INSERT INTO debezium.number_one_test values (" + (initialKey + 3) + ", -1, -1)");
        connection.executeWithoutCommitting("INSERT INTO debezium.number_one_test (id) values (" + (initialKey + 4) + ")");
        connection.execute("COMMIT");
    }
}
