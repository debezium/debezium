/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
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
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * Integration tests for BLOB data type support.
 *
 * @author Chris Cranford
 */
@SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "BLOB not supported by this mine mode")
public class OracleBlobDataTypesIT extends AbstractAsyncEngineConnectorTest {

    private static final byte[] BIN_DATA = readBinaryData("data/test_lob_data.json");

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();
    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "BLOB_TEST");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "BLOB_TEST");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldSnapshotBlobDataTypes() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "primary key(id))";

        connection.execute(ddl);

        String dml = "INSERT INTO BLOB_TEST VALUES (1, utl_raw.cast_to_raw('Hello World'))";
        connection.execute(dml);

        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidRead(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamSmallBlobDataTypeValues() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 100));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 0, 200));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?)", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 0, 300));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?)", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3));

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 201));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1Update));

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 202));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 303));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2Update));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3Update));

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamSmallBlobDataTypeValuesWithNonBlobFields() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 100));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?, 'Test1')", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 0, 200));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?, 'Test2')", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 0, 300));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?, 'Test3')", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 201));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test1U' WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 202));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test2U' WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 303));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test3U' WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamLargeBlobDataTypeValues() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 10, 24010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?)", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 50, 24050));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?)", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3));

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 24001));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1Update));

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 24002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 24003));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2Update));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3Update));

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamLargeBlobDataTypeValuesWithNonBlobFields() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?, 'Test1')", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 10, 24010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?, 'Test2')", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 50, 24050));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?, 'Test3')", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 24001));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test1U' WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 24002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test2U' WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 24003));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test3U' WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamMixedBlobDataTypeValuesWithNonBlobFieldsSameTable() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOBS blob, "
                + "VAL_BLOB blob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1a = createBlob(part(BIN_DATA, 1, 201));
        Blob blob1b = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?, ?, 'Test1')", p -> {
            p.setBlob(1, blob1a);
            p.setBlob(2, blob1b);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob1a));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1b));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Blob blob2a = createBlob(part(BIN_DATA, 10, 210));
        Blob blob2b = createBlob(part(BIN_DATA, 10, 24010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?, ?, 'Test2')", p -> {
            p.setBlob(1, blob2a);
            p.setBlob(2, blob2b);
        }, null);
        Blob blob3a = createBlob(part(BIN_DATA, 50, 250));
        Blob blob3b = createBlob(part(BIN_DATA, 50, 24050));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?, ?, 'Test3')", p -> {
            p.setBlob(1, blob3a);
            p.setBlob(2, blob3b);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob2a));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2b));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob3a));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3b));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Blob blob1aUpdate = createBlob(part(BIN_DATA, 5, 205));
        Blob blob1bUpdate = createBlob(part(BIN_DATA, 1, 24001));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blobs = ?, val_blob = ?, val_data = 'Test1U' WHERE id = 1", p -> {
            p.setBlob(1, blob1aUpdate);
            p.setBlob(2, blob1bUpdate);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob1aUpdate));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1bUpdate));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Blob blob2aUpdate = createBlob(part(BIN_DATA, 2, 202));
        Blob blob2bUpdate = createBlob(part(BIN_DATA, 2, 24002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blobs = ?, val_blob = ?, val_data = 'Test2U' WHERE id = 2", p -> {
            p.setBlob(1, blob2aUpdate);
            p.setBlob(2, blob2bUpdate);
        }, null);
        Blob blob3aUpdate = createBlob(part(BIN_DATA, 3, 203));
        Blob blob3bUpdate = createBlob(part(BIN_DATA, 3, 24003));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blobs = ?, val_blob = ?, val_data = 'Test3U' WHERE id = 3", p -> {
            p.setBlob(1, blob3aUpdate);
            p.setBlob(2, blob3bUpdate);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob2aUpdate));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob2bUpdate));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob3aUpdate));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob3bUpdate));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOBS")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.blob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOBS")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOBS")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor({ "DBZ-2948", "DBZ-5773" })
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "OpenLogReplicator does not differentiate between LOB operations")
    public void shouldNotStreamAnyChangesWhenLobEraseIsDetected() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        LogInterceptor logminerLogInterceptor = new LogInterceptor(TransactionCommitConsumer.class);
        final LogInterceptor xstreamLogInterceptor = new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));

        // Execute LOB_ERASE
        connection.execute("DECLARE loc_b BLOB; amount integer; BEGIN "
                + "SELECT \"VAL_BLOB\" INTO loc_b FROM BLOB_TEST WHERE ID = 1 for update; "
                + "amount := 10;"
                + "dbms_lob.erase(loc_b, amount, 1); end;");

        // Wait until the log has recorded the message.
        Awaitility.await().atMost(Duration.ofMinutes(1))
                .until(() -> logminerLogInterceptor.containsWarnMessage("LOB_ERASE for table")
                        || xstreamLogInterceptor.containsWarnMessage("LOB_ERASE for table"));
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor({ "DBZ-2948", "DBZ-5773" })
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.OLR, reason = "OpenLogReplicator does not differentiate between LOB operations")
    public void shouldStreamChangesWhenLobEraseIsDetected() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOB blob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1));

        // Execute LOB_ERASE
        connection.execute("DECLARE loc_b BLOB; amount integer; BEGIN "
                + "SELECT \"VAL_BLOB\" INTO loc_b FROM BLOB_TEST WHERE ID = 1 for update; "
                + "amount := 10;"
                + "dbms_lob.erase(loc_b, amount, 1); end;");

        // Wait until the log has recorded the message.
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamBlobFieldsWithPrimaryKeyChange() throws Exception {
        String ddl = "CREATE TABLE BLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_BLOBS blob, "
                + "VAL_BLOB blob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.blob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1a = createBlob(part(BIN_DATA, 1, 201));
        Blob blob1b = createBlob(part(BIN_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?, ?, 'Test1')", p -> {
            p.setBlob(1, blob1a);
            p.setBlob(2, blob1b);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob1a));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1b));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Update record, including primary key
        Blob blob1aUpdate = createBlob(part(BIN_DATA, 5, 205));
        Blob blob1bUpdate = createBlob(part(BIN_DATA, 1, 24001));
        connection.prepareQuery("UPDATE debezium.blob_test SET id = 2, val_blobs = ?, val_blob = ?, val_data = 'Test1U' WHERE id = 1", p -> {
            p.setBlob(1, blob1aUpdate);
            p.setBlob(2, blob1bUpdate);
        }, null);
        connection.commit();

        // 3 records, one indicating the deletion of PK 1, tombstone, and PK 2
        records = consumeRecordsByTopic(3);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(3);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(getByteBufferFromBlob(blob1aUpdate));
        assertThat(after.get("VAL_BLOB")).isEqualTo(getByteBufferFromBlob(blob1bUpdate));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");
    }

    @Test
    @FixFor("DBZ-3631")
    public void shouldReconcileTransactionWhenAllBlobClobAreInitializedAsNull() throws Exception {
        final String DDL = "CREATE TABLE dbz3631 ("
                + "ID NUMBER(38) NOT NULL,"
                + "ENTITY_ID NUMBER(38) NOT NULL,"
                + "DOCX BLOB,"
                + "DOCX_SIGNATURE BLOB,"
                + "XML_OOS BLOB,"
                + "XML_OOS_SIGNATURE BLOB,"
                + "PRIMARY KEY(ID))";

        TestHelper.dropTable(connection, "dbz3631");
        try {
            connection.execute(DDL);
            TestHelper.streamTable(connection, "dbz3631");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ3631")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Performs an insert with several blob fields, should produce an insert/update pair
            connection.executeWithoutCommitting("INSERT INTO dbz3631 ("
                    + "ID,"
                    + "ENTITY_ID"
                    + ") VALUES ("
                    + "13268281,"
                    + "13340568"
                    + ")");

            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> table = records.recordsForTopic("server1.DEBEZIUM.DBZ3631");
            assertThat(table).hasSize(1);

            SourceRecord record = table.get(0);
            Struct value = (Struct) record.value();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(13268281));
            assertThat(after.get("ENTITY_ID")).isEqualTo(BigDecimal.valueOf(13340568));
            assertThat(after.get("DOCX")).isNull();
            assertThat(after.get("DOCX_SIGNATURE")).isNull();
            assertThat(after.get("XML_OOS")).isNull();
            assertThat(after.get("XML_OOS_SIGNATURE")).isNull();
            assertThat(value.get(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.CREATE.code());
        }
        finally {
            TestHelper.dropTable(connection, "dbz3631");
        }
    }

    @Test
    @FixFor("DBZ-3645")
    public void shouldNotEmitBlobFieldValuesWhenLobSupportIsNotEnabled() throws Exception {
        boolean logMinerAdapter = TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER);
        TestHelper.dropTable(connection, "dbz3645");
        try {
            connection.execute("CREATE TABLE dbz3645 (id numeric(9,0), data blob, primary key(id))");
            TestHelper.streamTable(connection, "dbz3645");

            // Small data
            Blob blob1 = createBlob(part(BIN_DATA, 0, 250));
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (1,?)", ps -> ps.setBlob(1, blob1), null);

            // Large data
            Blob blob2 = createBlob(part(BIN_DATA, 0, 25000));
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (2,?)", ps -> ps.setBlob(1, blob2), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3645")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .with(OracleConnectorConfig.LOB_ENABLED, false)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Get snapshot records
            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            assertThat(table).hasSize(2);

            SourceRecord record = table.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            record = table.get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            // Small data and large data
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (3,?)", ps -> ps.setBlob(1, blob1), null);
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (4,?)", ps -> ps.setBlob(1, blob2), null);
            connection.commit();

            // Get streaming records
            sourceRecords = consumeRecordsByTopic(logMinerAdapter ? 3 : 2);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            assertThat(table).hasSize(logMinerAdapter ? 3 : 2);

            record = table.get(0);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            if (logMinerAdapter) {
                // With LogMiner, the first event only contains the initialization of id
                assertThat(after.get("DATA")).isNull();
            }
            else {
                // Xstream combines the insert and subsequent LogMiner update into a single insert event
                // automatically, so we receive the value here where the LogMiner implementation doesn't.
                assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob1));
            }
            assertThat(((Struct) record.value()).get("op")).isEqualTo("c");

            // LogMiner will pickup a separate update for BLOB fields.
            // There is no way to differentiate this change from any other UPDATE so the connector
            // will continue to emit it, but as a stand-alone UPDATE rather than merging it with
            // the parent INSERT as it would when LOB is enabled.
            if (logMinerAdapter) {
                record = table.get(1);
                after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(3);
                assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob1));
                assertThat(((Struct) record.value()).get("op")).isEqualTo("u");
            }

            record = table.get(logMinerAdapter ? 2 : 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            if (logMinerAdapter) {
                // the second insert won't emit an update due to the clob field being set by using the
                // SELECT_LOB_LOCATOR, LOB_WRITE, and LOB_TRIM operators when using LogMiner and the
                assertThat(after.get("DATA")).isNull();
            }
            else {
                // Xstream gets this value; it will be supplied.
                assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob2));
            }
            assertThat(((Struct) record.value()).get("op")).isEqualTo("c");

            // Test updates with small blob values
            Blob blob1u = createBlob(part(BIN_DATA, 5, 255));
            connection.prepareQuery("UPDATE dbz3645 set data=? WHERE id = 3", ps -> ps.setBlob(1, blob1u), null);
            connection.commit();

            sourceRecords = consumeRecordsByTopic(1);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidUpdate(table.get(0), "ID", 3);

            // When updating a table that contains a small BLOB value but the update does not modify
            // any of the non-BLOB fields, we expect the placeholder in the before and the value in the after.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(getAfterField(table.get(0), "DATA")).isEqualTo(getByteBufferFromBlob(blob1u));
            assertNoRecordsToConsume();

            // Test updates with large blob values
            Blob blob2u = createBlob(part(BIN_DATA, 5, 10000));
            connection.prepareQuery("UPDATE dbz3645 set data=? WHERE id = 4", ps -> ps.setBlob(1, blob2u), null);
            connection.commit();

            if (logMinerAdapter) {
                // When updating a table that contains a large BLOB value but the update does not modify
                // any of the non-BLOB fields, don't expect any events to be emitted. This is because
                // the event is treated as a SELECT_LOB_LOCATOR and LOB_WRITE series which is ignored.
                waitForAvailableRecords(5, TimeUnit.SECONDS);
            }
            else {
                // Xstream actually picks up this particular event.
                sourceRecords = consumeRecordsByTopic(1);
                table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
                VerifyRecord.isValidUpdate(table.get(0), "ID", 4);
                assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getAfterField(table.get(0), "DATA")).isEqualTo(getByteBufferFromBlob(blob2u));
            }

            assertNoRecordsToConsume();

            // Test update small blob row by changing non-blob fields
            connection.execute("UPDATE dbz3645 set id=5 where id=3");

            // Get streaming records
            // Expect 3 records: delete for ID=3, tombstone for ID=3, create for ID=5
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 3);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 3);
            VerifyRecord.isValidInsert(table.get(2), "ID", 5);

            // When updating a table that contains a small BLOB value but the update does not modify
            // any of the BLOB fields, we expect the placeholder.
            record = table.get(2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test update large blob row by changing non-blob fields
            connection.execute("UPDATE dbz3645 set id=6 where id=4");

            // Get streaming records
            // Expect 3 records: delete for ID=4, tombstone for ID=4, create for ID=6
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 4);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 4);
            VerifyRecord.isValidInsert(table.get(2), "ID", 6);

            // When updating a table that contains a large BLOB value but the update does not modify
            // any of the BLOB fields, we expect the placeholder.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test updating both small blob and non-blob fields
            Blob blob1u2 = createBlob(part(BIN_DATA, 10, 260));
            connection.prepareQuery("UPDATE dbz3645 SET data=?, id=7 WHERE id=5", ps -> ps.setBlob(1, blob1u2), null);
            connection.commit();

            // Get streaming records
            // The number of expected records depends on whether this test is using LogMiner or Xstream.
            // LogMiner expects 4: delete for ID=5, tombstone for ID=5, create for ID=7, update for ID=7
            // XStream expects 3: delete for ID=5, tombstone for ID=5, create for ID=7
            //
            // NOTE: The extra update event is because the BLOB value is treated inline and so LogMiner
            // does not emit a SELECT_LOB_LOCATOR event but rather a subsequent update that is captured
            // but not merged since event merging happens only when LOB is enabled.
            sourceRecords = consumeRecordsByTopic(logMinerAdapter ? 4 : 3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 5);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 5);
            VerifyRecord.isValidInsert(table.get(2), "ID", 7);

            if (logMinerAdapter) {
                VerifyRecord.isValidUpdate(table.get(3), "ID", 7);
            }

            // When updating a table's small blob and non-blob columns
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            if (logMinerAdapter) {
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getBeforeField(table.get(3), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getAfterField(table.get(3), "DATA")).isEqualTo(getByteBufferFromBlob(blob1u2));
            }
            else {
                // Xstream combines the insert/update into a single insert
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getByteBufferFromBlob(blob1u2));
            }
            assertNoRecordsToConsume();

            // Test updating both large blob and non-blob fields
            Blob blob2u2 = createBlob(part(BIN_DATA, 10, 12500));
            connection.prepareQuery("UPDATE dbz3645 SET data=?, id=8 WHERE id=6", ps -> ps.setBlob(1, blob2u2), null);
            connection.commit();

            // Get streaming records
            // Expect 3 records: delete for ID=6, tombstone for ID=6, create for ID=8
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 6);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 6);
            VerifyRecord.isValidInsert(table.get(2), "ID", 8);

            // When updating a table's large blob and non-blob columns, we expect placeholder in after
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            if (logMinerAdapter) {
                // LogMiner is unable to provide the value, so it gets emitted with the placeholder.
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            }
            else {
                // Xstream gets the value, so its provided.
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getByteBufferFromBlob(blob2u2));
            }
            assertNoRecordsToConsume();

            // Test deleting a row from a table with a small blob column
            connection.execute("DELETE FROM dbz3645 WHERE id=7");

            // Get streaming records
            // Expect 2 records: delete for ID=6, tombstone for ID=6
            sourceRecords = consumeRecordsByTopic(2);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 7);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 7);

            // when deleting, we expect placeholder
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test deleting a row from a table with a large blob column
            connection.execute("DELETE FROM dbz3645 WHERE id=8");

            // Get streaming records
            // Expect 2 records: delete for ID=6, tombstone for ID=6
            sourceRecords = consumeRecordsByTopic(2);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 8);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 8);

            // when deleting, we expect placeholder
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            // As a sanity, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3645");
        }
    }

    @Test
    @FixFor("DBZ-3893")
    public void shouldStreamNotNullBlobUsingEmptyBlobFunction() throws Exception {
        TestHelper.dropTable(connection, "dbz3898");
        try {
            connection.execute("CREATE TABLE dbz3898 (id numeric(9,0), data blob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz3898");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3898")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Empty function usage
            connection.execute("INSERT INTO dbz3898 (id,data) values (1,EMPTY_BLOB())");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ3898"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ3898")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap("".getBytes()));

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3898");
        }
    }

    @Test
    @FixFor("DBZ-4276")
    public void shouldStreamBlobWithUnavailableColumnValuePlaceholder() throws Exception {
        TestHelper.dropTable(connection, "dbz4276");
        try {
            connection.execute("CREATE TABLE dbz4276 (id numeric(9,0), data blob not null, data3 varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4276");

            // Empty function usage
            connection.execute("INSERT INTO dbz4276 (id,data,data3) values (1,EMPTY_BLOB(),'Test')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4276")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4276"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4276")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap("".getBytes()));
            assertThat(after.get("DATA3")).isEqualTo("Test");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Test unavailable column placeholder with update
            connection.execute("UPDATE dbz4276 set data3 = '123' WHERE id = 1");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4276"))).hasSize(1);

            record = records.recordsForTopic(topicName("DBZ4276")).get(0);
            Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(before.get("DATA3")).isEqualTo("Test");

            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(after.get("DATA3")).isEqualTo("123");

            // Test unavailable column placeholder with update
            connection.execute("DELETE FROM dbz4276 WHERE id = 1");

            records = consumeRecordsByTopic(2); // delete and tombstone
            assertThat(records.recordsForTopic(topicName("DBZ4276"))).hasSize(2);

            record = records.recordsForTopic(topicName("DBZ4276")).get(0);
            before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(before.get("DATA3")).isEqualTo("123");

            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4276");
        }
    }

    @Test
    @FixFor("DBZ-4366")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Xstream marks chunks as end of rows")
    public void shouldStreamBlobsWrittenInChunkedMode() throws Exception {
        TestHelper.dropTable(connection, "dbz4366");
        try {
            connection.execute("CREATE TABLE dbz4366 (id numeric(9,0), data blob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz4366");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4366")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,data) values (1,EMPTY_BLOB())");
            final String fillQuery = "DECLARE\n" +
                    "  loc BLOB;\n" +
                    "  i PLS_INTEGER;\n" +
                    "BEGIN\n" +
                    "  SELECT data into loc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc, DBMS_LOB.LOB_READWRITE);\n" +
                    "  FOR i IN 1..1024 LOOP\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc, 1024, ?);\n" +
                    "  END LOOP;\n" +
                    "  DBMS_LOB.CLOSE(loc);\n" +
                    "END;";
            connection.prepareQuery(fillQuery, ps -> ps.setBytes(1, part(BIN_DATA, 0, 1024)), null);
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4366"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4366")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            ByteBuffer data = (ByteBuffer) after.get("DATA");
            assertThat(data.array().length).isEqualTo(1024 * 1024);

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4366");
        }
    }

    @Test
    @FixFor("DBZ-4366")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Xstream marks chunks as end of rows")
    public void shouldStreamBlobsWrittenInInterleavedChunkedMode() throws Exception {
        TestHelper.dropTable(connection, "dbz4366");
        try {
            connection.execute("CREATE TABLE dbz4366 (id numeric(9,0), data blob not null, data2 blob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz4366");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4366")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,data,data2) values (1,EMPTY_BLOB(),EMPTY_BLOB())");
            final String fillQuery = "DECLARE\n" +
                    "  loc BLOB;\n" +
                    "  loc2 BLOB;\n" +
                    "  i PLS_INTEGER;\n" +
                    "BEGIN\n" +
                    "  FOR i IN 1..1024 LOOP\n" +
                    "    SELECT data into loc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "    DBMS_LOB.OPEN(loc, DBMS_LOB.LOB_READWRITE);\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc, 1024, ?);\n" +
                    "    DBMS_LOB.CLOSE(loc);\n" +
                    "    \n" +
                    "    SELECT data2 into loc2 FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "    DBMS_LOB.OPEN(loc2, DBMS_LOB.LOB_READWRITE);\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc2, 1024, ?);\n" +
                    "    DBMS_LOB.CLOSE(loc2);\n" +
                    "  END LOOP;\n" +
                    "END;";
            connection.prepareQuery(fillQuery, ps -> {
                ps.setBytes(1, part(BIN_DATA, 0, 1024));
                ps.setBytes(2, part(BIN_DATA, 0, 1024));
            }, null);
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4366"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4366")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            ByteBuffer data = (ByteBuffer) after.get("DATA");
            assertThat(data.array().length).isEqualTo(1024 * 1024);
            ByteBuffer data2 = (ByteBuffer) after.get("DATA2");
            assertThat(data2.array().length).isEqualTo(1024 * 1024);

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4366");
        }
    }

    @Test
    @FixFor("DBZ-4366")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Xstream marks chunks as end of rows")
    public void shouldStreamBlobsWrittenInInterleavedChunkedMode2() throws Exception {
        TestHelper.dropTable(connection, "dbz4366");
        try {
            connection.execute("CREATE TABLE dbz4366 (id numeric(9,0), data blob not null, data2 blob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz4366");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4366")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,data,data2) values (1,EMPTY_BLOB(),EMPTY_BLOB())");
            final String fillQuery = "DECLARE\n" +
                    "  loc BLOB;\n" +
                    "  loc2 BLOB;\n" +
                    "  i PLS_INTEGER;\n" +
                    "BEGIN\n" +
                    "  SELECT data into loc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc, DBMS_LOB.LOB_READWRITE);\n" +
                    "  SELECT data2 into loc2 FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc2, DBMS_LOB.LOB_READWRITE);\n" +
                    "  FOR i IN 1..1024 LOOP\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc, 1024, ?);\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc2, 1024, ?);\n" +
                    "  END LOOP;\n" +
                    "  DBMS_LOB.CLOSE(loc);\n" +
                    "  DBMS_LOB.CLOSE(loc2);\n" +
                    "END;";
            connection.prepareQuery(fillQuery, ps -> {
                ps.setBytes(1, part(BIN_DATA, 0, 1024));
                ps.setBytes(2, part(BIN_DATA, 0, 1024));
            }, null);
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4366"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4366")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            ByteBuffer data = (ByteBuffer) after.get("DATA");
            assertThat(data.array().length).isEqualTo(1024 * 1024);
            ByteBuffer data2 = (ByteBuffer) after.get("DATA2");
            assertThat(data2.array().length).isEqualTo(1024 * 1024);

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4366");
        }
    }

    @Test
    @FixFor("DBZ-4366")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Xstream marks chunks as end of rows")
    public void shouldStreamBlobsWrittenInInterleavedChunkedMode3() throws Exception {
        TestHelper.dropTable(connection, "dbz4366");
        try {
            connection.execute("CREATE TABLE dbz4366 (id numeric(9,0), data blob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz4366");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4366")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,data) values (1,EMPTY_BLOB())");
            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,data) values (2,EMPTY_BLOB())");
            final String fillQuery = "DECLARE\n" +
                    "  loc BLOB;\n" +
                    "  loc2 BLOB;\n" +
                    "  i PLS_INTEGER;\n" +
                    "BEGIN\n" +
                    "  SELECT data into loc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc, DBMS_LOB.LOB_READWRITE);\n" +
                    "  SELECT data into loc2 FROM dbz4366 WHERE id = 2 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc2, DBMS_LOB.LOB_READWRITE);\n" +
                    "  FOR i IN 1..1024 LOOP\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc, 1024, ?);\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc2, 1024, ?);\n" +
                    "  END LOOP;\n" +
                    "  DBMS_LOB.CLOSE(loc);\n" +
                    "  DBMS_LOB.CLOSE(loc2);\n" +
                    "END;";
            connection.prepareQuery(fillQuery, ps -> {
                ps.setBytes(1, part(BIN_DATA, 0, 1024));
                ps.setBytes(2, part(BIN_DATA, 0, 1024));
            }, null);
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic(topicName("DBZ4366"))).hasSize(2);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4366")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            ByteBuffer data = (ByteBuffer) after.get("DATA");
            assertThat(data.array().length).isEqualTo(1024 * 1024);

            record = records.recordsForTopic(topicName("DBZ4366")).get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            data = (ByteBuffer) after.get("DATA");
            assertThat(data.array().length).isEqualTo(1024 * 1024);

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4366");
        }
    }

    @Test
    @FixFor("DBZ-5295")
    public void shouldReselectBlobAfterPrimaryKeyChange() throws Exception {
        TestHelper.dropTable(connection, "dbz5295");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BaseChangeRecordEmitter.class);
            logInterceptor.setLoggerLevel(BaseChangeRecordEmitter.class, Level.INFO);

            connection.execute("create table dbz5295 (id numeric(9,0) primary key, data blob)");
            TestHelper.streamTable(connection, "dbz5295");

            Blob blob = createBlob(part(BIN_DATA, 0, 1024));
            connection.prepareQuery("INSERT INTO dbz5295 (id,data) values (1,?)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5295")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("DBZ5295"));
            assertThat(recordsForTopic).hasSize(1);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob));

            connection.execute("UPDATE dbz5295 set id = 2 where id = 1");

            // The update of the primary key causes a DELETE and a CREATE
            records = consumeRecordsByTopic(3);
            recordsForTopic = records.recordsForTopic(topicName("DBZ5295"));
            assertThat(recordsForTopic).hasSize(3);

            // First event is a delete
            record = recordsForTopic.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Second event is the tombstone
            record = recordsForTopic.get(1);
            VerifyRecord.isValidTombstone(record);

            // Third event is the create
            record = recordsForTopic.get(2);
            VerifyRecord.isValidInsert(record, "ID", 2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob));

            assertThat(logInterceptor.containsMessage("re-selecting LOB columns [DATA] out of bands")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5295");
        }
    }

    @Test
    @FixFor("DBZ-5295")
    public void shouldReselectBlobAfterPrimaryKeyChangeWithRowDeletion() throws Exception {
        TestHelper.dropTable(connection, "dbz5295");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BaseChangeRecordEmitter.class);
            logInterceptor.setLoggerLevel(BaseChangeRecordEmitter.class, Level.INFO);

            connection.execute("create table dbz5295 (id numeric(9,0) primary key, data blob)");
            TestHelper.streamTable(connection, "dbz5295");

            Blob blob = createBlob(part(BIN_DATA, 0, 1024));
            connection.prepareQuery("INSERT INTO dbz5295 (id,data) values (1,?)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5295")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("DBZ5295"));
            assertThat(recordsForTopic).hasSize(1);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(getByteBufferFromBlob(blob));

            // Update the PK and then delete the row within the same transaction
            connection.executeWithoutCommitting("UPDATE dbz5295 set id = 2 where id = 1");
            connection.execute("DELETE FROM dbz5295 where id = 2");

            // The update of the primary key causes a DELETE and a CREATE, mingled with a TOMBSTONE
            records = consumeRecordsByTopic(4);
            recordsForTopic = records.recordsForTopic(topicName("DBZ5295"));
            assertThat(recordsForTopic).hasSize(4);

            // First event: DELETE
            record = recordsForTopic.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Second event: TOMBSTONE
            record = recordsForTopic.get(1);
            VerifyRecord.isValidTombstone(record);

            // Third event: CREATE
            record = recordsForTopic.get(2);
            VerifyRecord.isValidInsert(record, "ID", 2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            // Fourth event: DELETE
            record = recordsForTopic.get(3);
            VerifyRecord.isValidDelete(record, "ID", 2);
            Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            assertThat(logInterceptor.containsMessage("re-selecting LOB columns [DATA] out of bands")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5295");
        }
    }

    @Test
    @FixFor("DBZ-7456")
    public void shouldNotReselectBlobAfterPrimaryKeyChangeColumnExcluded() throws Exception {
        TestHelper.dropTable(connection, "dbz7456");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BaseChangeRecordEmitter.class);
            logInterceptor.setLoggerLevel(BaseChangeRecordEmitter.class, Level.INFO);

            connection.execute("create table dbz7456 (id numeric(9,0) primary key, data blob)");
            TestHelper.streamTable(connection, "dbz7456");

            Blob blob = createBlob(part(BIN_DATA, 0, 1024));
            connection.prepareQuery("INSERT INTO dbz7456 (id,data) values (1,?)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7456")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .with(OracleConnectorConfig.COLUMN_EXCLUDE_LIST, "DEBEZIUM.DBZ7456.DATA")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("DBZ7456"));
            assertThat(recordsForTopic).hasSize(1);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.schema().field("DATA")).isNull();

            connection.execute("UPDATE dbz7456 set id = 2 where id = 1");

            // The update of the primary key causes a DELETE and a CREATE
            records = consumeRecordsByTopic(3);
            recordsForTopic = records.recordsForTopic(topicName("DBZ7456"));
            assertThat(recordsForTopic).hasSize(3);

            // First event is a delete
            record = recordsForTopic.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Second event is the tombstone
            record = recordsForTopic.get(1);
            VerifyRecord.isValidTombstone(record);

            // Third event is the create
            record = recordsForTopic.get(2);
            VerifyRecord.isValidInsert(record, "ID", 2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.schema().field("DATA")).isNull();

            assertThat(logInterceptor.containsMessage("re-selecting LOB columns [DATA] out of bands")).isFalse();
        }
        finally {
            TestHelper.dropTable(connection, "dbz7456");
        }
    }

    @Test
    @FixFor("DBZ-7456")
    public void shouldNotReselectBlobAfterPrimaryKeyChangeWithRowDeletionColumnExcluded() throws Exception {
        TestHelper.dropTable(connection, "dbz7456");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BaseChangeRecordEmitter.class);
            logInterceptor.setLoggerLevel(BaseChangeRecordEmitter.class, Level.INFO);

            connection.execute("create table dbz7456 (id numeric(9,0) primary key, data blob)");
            TestHelper.streamTable(connection, "dbz7456");

            Blob blob = createBlob(part(BIN_DATA, 0, 1024));
            connection.prepareQuery("INSERT INTO dbz7456 (id,data) values (1,?)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7456")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .with(OracleConnectorConfig.COLUMN_EXCLUDE_LIST, "DEBEZIUM.DBZ7456.DATA")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("DBZ7456"));
            assertThat(recordsForTopic).hasSize(1);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.schema().field("DATA")).isNull();

            // Update the PK and then delete the row within the same transaction
            connection.executeWithoutCommitting("UPDATE dbz7456 set id = 2 where id = 1");
            connection.execute("DELETE FROM dbz7456 where id = 2");

            // The update of the primary key causes a DELETE and a CREATE, mingled with a TOMBSTONE
            records = consumeRecordsByTopic(4);
            recordsForTopic = records.recordsForTopic(topicName("DBZ7456"));
            assertThat(recordsForTopic).hasSize(4);

            // First event: DELETE
            record = recordsForTopic.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Second event: TOMBSTONE
            record = recordsForTopic.get(1);
            VerifyRecord.isValidTombstone(record);

            // Third event: CREATE
            record = recordsForTopic.get(2);
            VerifyRecord.isValidInsert(record, "ID", 2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.schema().field("DATA")).isNull();

            // Fourth event: DELETE
            record = recordsForTopic.get(3);
            VerifyRecord.isValidDelete(record, "ID", 2);
            Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.schema().field("DATA")).isNull();

            assertThat(logInterceptor.containsMessage("re-selecting LOB columns [DATA] out of bands")).isFalse();
        }
        finally {
            TestHelper.dropTable(connection, "dbz7456");
        }
    }

    @Test
    @FixFor("DBZ-5581")
    public void testBlobUnavailableValuePlaceholderUpdateOnlyOneBlobColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz5581");
        try {
            connection.execute("create table dbz5581 (id numeric(9,0) primary key, a1 varchar2(200), a2 blob, a3 blob, a4 varchar2(100))");
            TestHelper.streamTable(connection, "dbz5581");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5581")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final Blob a2 = createBlob(part(BIN_DATA, 0, 4100));
            final Blob a3 = createBlob(part(BIN_DATA, 0, 4100));
            connection.prepareQuery("INSERT into dbz5581 (id,a1,a2,a3,a4) values (1, 'lwmzVQd6r7', ?, ?, 'cuTVQV0OpK')", st -> {
                st.setBlob(1, a2);
                st.setBlob(2, a3);
            }, null);
            connection.commit();

            final Blob a2u = createBlob(part(BIN_DATA, 1, 4101));
            connection.prepareQuery("UPDATE dbz5581 set A2=? WHERE ID=1", st -> st.setBlob(1, a2u), null);
            connection.commit();

            connection.execute("UPDATE dbz5581 set A2=NULL WHERE ID=1");

            SourceRecords records = consumeRecordsByTopic(3);
            List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DBZ5581");
            assertThat(recordsForTopic).hasSize(3);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A1")).isEqualTo("lwmzVQd6r7");
            assertThat(after.get("A2")).isEqualTo(getByteBufferFromBlob(a2));
            assertThat(after.get("A3")).isEqualTo(getByteBufferFromBlob(a3));
            assertThat(after.get("A4")).isEqualTo("cuTVQV0OpK");

            record = recordsForTopic.get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A1")).isEqualTo("lwmzVQd6r7");
            assertThat(after.get("A2")).isEqualTo(getByteBufferFromBlob(a2u));
            assertThat(after.get("A3")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(after.get("A4")).isEqualTo("cuTVQV0OpK");

            record = recordsForTopic.get(2);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A1")).isEqualTo("lwmzVQd6r7");
            assertThat(after.get("A2")).isNull();
            assertThat(after.get("A3")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(after.get("A4")).isEqualTo("cuTVQV0OpK");

        }
        finally {
            TestHelper.dropTable(connection, "dbz5581");
        }
    }

    @Test
    @FixFor("DBZ-7790")
    public void shouldNotMergeBlobDataWhenNoPrimaryKey() throws Exception {
        TestHelper.dropTable(connection, "DBZ7790");
        try {
            connection.execute("CREATE TABLE DBZ7790(id numeric(9,0), DATA BLOB)");
            TestHelper.streamTable(connection, "DBZ7790");

            final Blob snapshotBlob1 = createBlob("aaa".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO DBZ7790 values (1,?)", ps -> ps.setBlob(1, snapshotBlob1), null);
            connection.commit();

            final Blob snapshotBlob2 = createBlob("bbb".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO DBZ7790 values (2,?)", ps -> ps.setBlob(1, snapshotBlob2), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7790")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Get snapshot records
            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName("DBZ7790"));
            assertThat(tableRecords).hasSize(2);

            SourceRecord insert1 = tableRecords.get(0);
            assertThat(getAfterField(insert1, "ID")).isEqualTo(1);
            assertThat(getAfterField(insert1, "DATA")).isEqualTo(getByteBufferFromBlob(snapshotBlob1));

            SourceRecord insert2 = tableRecords.get(1);
            assertThat(getAfterField(insert2, "ID")).isEqualTo(2);
            assertThat(getAfterField(insert2, "DATA")).isEqualTo(getByteBufferFromBlob(snapshotBlob2));

            // Update - streaming
            final Blob updateBlob1 = createBlob("ccc".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("UPDATE DBZ7790 set data = ? WHERE id = 1", ps -> ps.setBlob(1, updateBlob1), null);

            final Blob updateBlob2 = createBlob("ddd".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("UPDATE DBZ7790 set data = ? WHERE id = 2", ps -> ps.setBlob(1, updateBlob2), null);
            connection.commit();

            sourceRecords = consumeRecordsByTopic(2);
            tableRecords = sourceRecords.recordsForTopic(topicName("DBZ7790"));

            // Streaming
            assertThat(tableRecords).hasSize(2);

            SourceRecord update1 = tableRecords.get(0);
            assertThat(getAfterField(update1, "ID")).isEqualTo(1);
            assertThat(getAfterField(update1, "DATA")).isEqualTo(getByteBufferFromBlob(updateBlob1));

            SourceRecord update2 = tableRecords.get(1);
            assertThat(getAfterField(update2, "ID")).isEqualTo(2);
            assertThat(getAfterField(update2, "DATA")).isEqualTo(getByteBufferFromBlob(updateBlob2));

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "DBZ7790");
        }
    }

    @Test
    @FixFor("DBZ-7790")
    public void shouldNotMergeLargeBlobDataWhenNoPrimaryKey() throws Exception {
        TestHelper.dropTable(connection, "DBZ7790");
        try {
            connection.execute("CREATE TABLE DBZ7790(id numeric(9,0), DATA BLOB)");
            TestHelper.streamTable(connection, "DBZ7790");

            final Blob snapshotBlob1 = createBlob("aaa".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO DBZ7790 values (1,?)", ps -> ps.setBlob(1, snapshotBlob1), null);
            connection.commit();

            final Blob snapshotBlob2 = createBlob("bbb".getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO DBZ7790 values (2,?)", ps -> ps.setBlob(1, snapshotBlob2), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7790")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Get snapshot records
            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName("DBZ7790"));
            assertThat(tableRecords).hasSize(2);

            SourceRecord insert1 = tableRecords.get(0);
            assertThat(getAfterField(insert1, "ID")).isEqualTo(1);
            assertThat(getAfterField(insert1, "DATA")).isEqualTo(getByteBufferFromBlob(snapshotBlob1));

            SourceRecord insert2 = tableRecords.get(1);
            assertThat(getAfterField(insert2, "ID")).isEqualTo(2);
            assertThat(getAfterField(insert2, "DATA")).isEqualTo(getByteBufferFromBlob(snapshotBlob2));

            // Update - streaming
            final Blob updateBlob1 = createBlob(part(BIN_DATA, 1, 5000));
            connection.prepareQuery("UPDATE DBZ7790 set data = ? WHERE id = 1", ps -> ps.setBlob(1, updateBlob1), null);

            final Blob updateBlob2 = createBlob(part(BIN_DATA, 250, 5500));
            connection.prepareQuery("UPDATE DBZ7790 set data = ? WHERE id = 2", ps -> ps.setBlob(1, updateBlob2), null);
            connection.commit();

            sourceRecords = consumeRecordsByTopic(2);
            tableRecords = sourceRecords.recordsForTopic(topicName("DBZ7790"));

            // Streaming
            assertThat(tableRecords).hasSize(2);

            SourceRecord update1 = tableRecords.get(0);
            assertThat(getAfterField(update1, "ID")).isEqualTo(1);
            assertThat(getAfterField(update1, "DATA")).isEqualTo(getByteBufferFromBlob(updateBlob1));

            SourceRecord update2 = tableRecords.get(1);
            assertThat(getAfterField(update2, "ID")).isEqualTo(2);
            assertThat(getAfterField(update2, "DATA")).isEqualTo(getByteBufferFromBlob(updateBlob2));

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "DBZ7790");
        }
    }

    private static byte[] part(byte[] buffer, int start, int length) {
        return Arrays.copyOfRange(buffer, start, length);
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }

    private static byte[] readBinaryData(String pathOnClasspath) {
        try (InputStream stream = Testing.Files.readResourceAsStream(pathOnClasspath)) {
            return IoUtil.readBytes(stream);
        }
        catch (IOException e) {
            fail("Unable to read '" + pathOnClasspath + "'", e);
            return null;
        }
    }

    private Blob createBlob(byte[] data) throws SQLException {
        final Blob blob = connection.connection().createBlob();
        blob.setBytes(1, data);
        return blob;
    }

    private static ByteBuffer getByteBufferFromBlob(Blob blob) throws SQLException {
        return ByteBuffer.wrap(blob.getBytes(1, (int) blob.length()));
    }

    private static ByteBuffer getUnavailableValuePlaceholder(Configuration config) {
        return ByteBuffer.wrap(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER).getBytes());
    }

    private static Object getBeforeField(SourceRecord record, String fieldName) {
        return before(record).get(fieldName);
    }

    private static Object getAfterField(SourceRecord record, String fieldName) {
        return after(record).get(fieldName);
    }
}
