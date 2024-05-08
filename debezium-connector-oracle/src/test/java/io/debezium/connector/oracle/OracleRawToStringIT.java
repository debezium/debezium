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
import io.debezium.connector.oracle.converters.RawToStringConverter;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests the behavior of the {@link io.debezium.connector.oracle.converters.RawToStringConverter}.
 *
 * @author Chris Cranford
 */
public class OracleRawToStringIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.raw_to_string_test");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute("CREATE TABLE raw_to_string_test (id numeric primary key, data raw(128))");
        TestHelper.streamTable(connection, "debezium.raw_to_string_test");
    }

    @After
    public void after() throws Exception {
        stopConnector();

        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "debezium.raw_to_string_test");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-7753")
    public void shouldHandleRawConvertedToString() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.RAW_TO_STRING_TEST")
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "raw")
                .with("raw.type", RawToStringConverter.class.getName())
                .with("raw.selector", ".*RAW_TO_STRING_TEST\\.DATA.*")
                .build();

        // Insert snapshot data
        insertDataBatchStartingAtKey(1);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Consume snapshot records
        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> testRecords = records.recordsForTopic("server1.DEBEZIUM.RAW_TO_STRING_TEST");
        assertThat(testRecords).hasSize(2);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(1), null);
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(2), "2b09ccae-06aa-45aa-a2f1-dd0e4c047249");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert stream data
        insertDataBatchStartingAtKey(5);

        // Consume stream records
        records = consumeRecordsByTopic(2);
        testRecords = records.recordsForTopic("server1.DEBEZIUM.RAW_TO_STRING_TEST");
        assertThat(testRecords).hasSize(2);
        assertRecordNumberValue(testRecords, 0, BigDecimal.valueOf(5), null);
        assertRecordNumberValue(testRecords, 1, BigDecimal.valueOf(6), "2b09ccae-06aa-45aa-a2f1-dd0e4c047249");
    }

    private void assertRecordNumberValue(List<SourceRecord> records, int index, Object key, Object data) {
        final Struct after = (Struct) ((Struct) records.get(index).value()).get(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(key);
        assertThat(after.get("DATA")).isEqualTo(data);
    }

    private void insertDataBatchStartingAtKey(int initialKey) throws SQLException {
        connection.executeWithoutCommitting("INSERT INTO debezium.raw_to_string_test values (" + initialKey + ", null)");
        connection.prepareQuery("INSERT INTO debezium.raw_to_string_test values (?, utl_raw.cast_to_raw(?))", ps -> {
            ps.setInt(1, initialKey + 1);
            ps.setString(2, "2b09ccae-06aa-45aa-a2f1-dd0e4c047249");
        }, null);
        connection.commit();
    }
}
