/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
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
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
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
@SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.XSTREAM, reason = "XStream does not yet support BLOB data types")
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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1.getBytes(1, 100)));

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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2.getBytes(1, 200)));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3.getBytes(1, 300)));

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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1Update.getBytes(1, 200)));

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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2Update.getBytes(1, 200)));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3Update.getBytes(1, 300)));

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isNull();

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

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isNull();

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isNull();

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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1.getBytes(1, 100)));
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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2.getBytes(1, 200)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3.getBytes(1, 300)));
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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1Update.getBytes(1, 200)));
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
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2Update.getBytes(1, 200)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3Update.getBytes(1, 300)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isNull();
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

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isNull();
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isNull();
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
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 4000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1.getBytes(1, 4000)));

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 10, 4010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?)", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 50, 4050));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?)", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2.getBytes(1, 4000)));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3.getBytes(1, 4000)));

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 4001));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1Update.getBytes(1, 4000)));

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 4002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 4003));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ? WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2Update.getBytes(1, 4000)));

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3Update.getBytes(1, 4000)));

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isNull();

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

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isNull();

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isNull();

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
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 4000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?, 'Test1')", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Blob blob2 = createBlob(part(BIN_DATA, 10, 4010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?, 'Test2')", p -> p.setBlob(1, blob2), null);
        Blob blob3 = createBlob(part(BIN_DATA, 50, 4050));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (3, ?, 'Test3')", p -> p.setBlob(1, blob3), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Blob blob1Update = createBlob(part(BIN_DATA, 1, 4001));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test1U' WHERE id = 1", p -> p.setBlob(1, blob1Update), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1Update.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Blob blob2Update = createBlob(part(BIN_DATA, 2, 4002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test2U' WHERE id = 2", p -> p.setBlob(1, blob2Update), null);
        Blob blob3Update = createBlob(part(BIN_DATA, 3, 4003));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blob = ?, val_data = 'Test3U' WHERE id = 3", p -> p.setBlob(1, blob3Update), null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2Update.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3Update.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOB")).isNull();
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

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOB")).isNull();
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOB")).isNull();
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
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1a = createBlob(part(BIN_DATA, 1, 201));
        Blob blob1b = createBlob(part(BIN_DATA, 0, 4000));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob1a.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1b.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Blob blob2a = createBlob(part(BIN_DATA, 10, 210));
        Blob blob2b = createBlob(part(BIN_DATA, 10, 4010));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (2, ?, ?, 'Test2')", p -> {
            p.setBlob(1, blob2a);
            p.setBlob(2, blob2b);
        }, null);
        Blob blob3a = createBlob(part(BIN_DATA, 50, 250));
        Blob blob3b = createBlob(part(BIN_DATA, 50, 4050));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob2a.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2b.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob3a.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3b.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Blob blob1aUpdate = createBlob(part(BIN_DATA, 5, 205));
        Blob blob1bUpdate = createBlob(part(BIN_DATA, 1, 4001));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob1aUpdate.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1bUpdate.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Blob blob2aUpdate = createBlob(part(BIN_DATA, 2, 202));
        Blob blob2bUpdate = createBlob(part(BIN_DATA, 2, 4002));
        connection.prepareQuery("UPDATE debezium.blob_test SET val_blobs = ?, val_blob = ?, val_data = 'Test2U' WHERE id = 2", p -> {
            p.setBlob(1, blob2aUpdate);
            p.setBlob(2, blob2bUpdate);
        }, null);
        Blob blob3aUpdate = createBlob(part(BIN_DATA, 3, 203));
        Blob blob3bUpdate = createBlob(part(BIN_DATA, 3, 4003));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob2aUpdate.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob2bUpdate.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob3aUpdate.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob3bUpdate.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.blob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // blob fields will never have a "before" state
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_BLOBS")).isNull();
        assertThat(before.get("VAL_BLOB")).isNull();
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

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_BLOBS")).isNull();
        assertThat(before.get("VAL_BLOB")).isNull();
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("BLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // blob fields will never have a "before" state
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_BLOBS")).isNull();
        assertThat(before.get("VAL_BLOB")).isNull();
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
                .build();

        LogInterceptor logInterceptor = new LogInterceptor();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1 = createBlob(part(BIN_DATA, 0, 4000));
        connection.prepareQuery("INSERT INTO debezium.blob_test values (1, ?)", p -> p.setBlob(1, blob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("BLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("BLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1.getBytes(1, 4000)));

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
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Blob blob1a = createBlob(part(BIN_DATA, 1, 201));
        Blob blob1b = createBlob(part(BIN_DATA, 0, 4000));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob1a.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1b.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Update record, including primary key
        Blob blob1aUpdate = createBlob(part(BIN_DATA, 5, 205));
        Blob blob1bUpdate = createBlob(part(BIN_DATA, 1, 4001));
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
        assertThat(after.get("VAL_BLOBS")).isEqualTo(ByteBuffer.wrap(blob1aUpdate.getBytes(1, 200)));
        assertThat(after.get("VAL_BLOB")).isEqualTo(ByteBuffer.wrap(blob1bUpdate.getBytes(1, 4000)));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");
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
}
