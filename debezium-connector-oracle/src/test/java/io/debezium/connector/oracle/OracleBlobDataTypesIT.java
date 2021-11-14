/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

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
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Integration tests for BLOB data type support.
 *
 * @author Chris Cranford
 */
public class OracleBlobDataTypesIT extends AbstractConnectorTest {

    private static final byte[] BIN_DATA = readBinaryData("data/test_lob_data.json");

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "BLOB_TEST");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
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
        records.forEach(System.out::println);

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
        records.forEach(System.out::println);

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
        records.forEach(System.out::println);

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
    @FixFor("DBZ-2948")
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

        LogInterceptor logInterceptor = new LogInterceptor();
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
        Awaitility.await().atMost(Duration.ofMinutes(1)).until(() -> logInterceptor.containsWarnMessage("LOB_ERASE for table"));
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
            assertThat(after.get("DATA")).isNull();

            record = table.get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isNull();

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
            assertThat(after.get("DATA")).isNull();
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

            // the second insert won't emit an update due to the blob field being set by using the
            // SELECT_LOB_LOCATOR, LOB_WRITE, and LOB_TRIM operators when using LogMiner and the
            // BLOB field will be excluded automatically by Xstream due to skipping chunk processing.
            record = table.get(logMinerAdapter ? 2 : 1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            assertThat(after.get("DATA")).isNull();
            assertThat(((Struct) record.value()).get("op")).isEqualTo("c");

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
            Fail.fail("Unable to read '" + pathOnClasspath + "'", e);
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
}
