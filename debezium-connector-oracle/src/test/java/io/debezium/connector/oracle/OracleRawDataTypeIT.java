/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.Clob;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration tests for RAW data type support.
 *
 * @author Chris Cranford
 */
public class OracleRawDataTypeIT extends AbstractAsyncEngineConnectorTest {

    private static final int RAW_LENGTH = 2000;

    private static final String RAW_DATA = Files.readResourceAsString("data/test_lob_data.json");

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();
    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldSnapshotTableWithRawColumnType() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA raw(2000), primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            final String data = part(RAW_DATA, 0, RAW_LENGTH);
            connection.prepareQuery("insert into dbz3605 values (1,utl_raw.cast_to_raw(?))", ps -> ps.setString(1, data), null);
            connection.commit();

            Configuration config = getDefaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithRawColumnType() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA raw(2000), primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = part(RAW_DATA, 0, RAW_LENGTH);
            connection.prepareQuery("insert into dbz3605 values (1,utl_raw.cast_to_raw(?))", ps -> ps.setString(1, data), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));

            final String updateData = part(RAW_DATA, 500, RAW_LENGTH);
            connection.prepareQuery("UPDATE dbz3605 SET data = utl_raw.cast_to_raw(?) WHERE id = 1", ps -> ps.setString(1, updateData), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithRawTypeColumnAndOtherNonLobColumns() throws Exception {
        // This tests makes sure there are no special requirements when a table is keyless
        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA raw(2000), DATA2 varchar2(50))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = part(RAW_DATA, 0, RAW_LENGTH);
            connection.prepareQuery("insert into dbz3605 values (1,utl_raw.cast_to_raw(?),'Acme')", ps -> ps.setString(1, data), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            // Update only RAW
            final String updateData = part(RAW_DATA, 500, RAW_LENGTH);
            connection.prepareQuery("UPDATE dbz3605 SET data = utl_raw.cast_to_raw(?) WHERE id=1", ps -> ps.setString(1, updateData), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            // Update RAW and non-RAW
            connection.prepareQuery("UPDATE dbz3605 SET data = utl_raw.cast_to_raw(?), DATA2 = 'Data' WHERE id=1", ps -> ps.setString(1, data), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo("Data");

            // Update only non-RAW
            connection.execute("UPDATE dbz3605 SET DATA2 = 'Acme' WHERE id=1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithNoPrimaryKeyWithRawTypeColumn() throws Exception {
        // This tests makes sure there are no special requirements when a table is keyless
        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA raw(2000))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = part(RAW_DATA, 0, RAW_LENGTH);
            connection.prepareQuery("insert into dbz3605 values (1,utl_raw.cast_to_raw(?))", ps -> ps.setString(1, data), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));

            final String updateData = part(RAW_DATA, 500, RAW_LENGTH);
            connection.prepareQuery("UPDATE dbz3605 SET data = utl_raw.cast_to_raw(?) WHERE id=1", ps -> ps.setObject(1, updateData), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldStreamTableWithRawTypeColumnAndAnotherLobColumn() throws Exception {
        // For simplicity, pair large raw with a large CLOB data column for multi-fragment processing
        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA raw(2000), DATA2 clob)");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = part(RAW_DATA, 0, RAW_LENGTH);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, data);
            connection.prepareQuery("insert into dbz3605 values (1,utl_raw.cast_to_raw(?),?)",
                    ps -> {
                        ps.setString(1, data);
                        ps.setClob(2, clob);
                    }, null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes()));
            assertThat(after.get("DATA2")).isEqualTo(clob.getSubString(1, (int) clob.length()));

            final String updateData = part(RAW_DATA, 500, RAW_LENGTH);
            connection.prepareQuery("UPDATE dbz3605 SET data = utl_raw.cast_to_raw(?) WHERE id=1", ps -> ps.setObject(1, updateData), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));
            assertFieldIsUnavailablePlaceholder(after, "DATA2", config);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(updateData.getBytes()));
            assertFieldIsUnavailablePlaceholder(after, "DATA2", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    private Configuration.Builder getDefaultConfig() {
        return TestHelper.defaultConfig();
    }

    private static void assertFieldIsUnavailablePlaceholder(Struct after, String fieldName, Configuration config) {
        assertThat(after.getString(fieldName)).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static String part(String text, int start, int length) {
        return text == null ? "" : text.substring(start, Math.min(length, text.length()));
    }

}
