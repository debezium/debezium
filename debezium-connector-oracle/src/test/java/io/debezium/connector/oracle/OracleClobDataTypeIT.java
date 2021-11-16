/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.time.Duration;
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
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Integration tests for CLOB data type support.
 *
 * @author Chris Cranford
 */
public class OracleClobDataTypeIT extends AbstractConnectorTest {

    private static final String JSON_DATA = Testing.Files.readResourceAsString("data/test_lob_data.json");
    private static final String JSON_DATA2 = Testing.Files.readResourceAsString("data/test_lob_data2.json");

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "CLOB_TEST");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "CLOB_TEST");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldSnapshotClobDataTypeValues() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB_SHORT clob, "
                + "VAL_CLOB_LONG clob, "
                + "VAL_NCLOB_SHORT nclob, "
                + "VAL_NCLOB_LONG nclob, "
                + "primary key(id))";

        connection.execute(ddl);

        Clob clob1 = createClob("Hello World");
        Clob clob2 = createClob(part(JSON_DATA, 0, 5000));
        NClob nclob1 = createNClob("Hello World");
        NClob nclob2 = createNClob(part(JSON_DATA2, 0, 5000));

        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (1, ?, ?, ?, ?)", ps -> {
            ps.setClob(1, clob1);
            ps.setClob(2, clob2);
            ps.setNClob(3, nclob1);
            ps.setNClob(4, nclob2);
        }, null);
        connection.commit();

        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidRead(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB_SHORT")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_CLOB_LONG")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB_SHORT")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_NCLOB_LONG")).isEqualTo(getClobString(nclob2));
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamInlineClobDataTypeValues() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 1000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 1000));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (1, ?, ?)", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 1000));
        NClob nclob2 = createNClob(part(JSON_DATA2, 1, 1000));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (2, ?, ?)", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 2, 1000));
        NClob nclob3 = createNClob(part(JSON_DATA2, 2, 1000));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (3, ?, ?)", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));

        // Update record
        Clob clob1Update = createClob(part(JSON_DATA, 1, 1000));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 1000));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 0, 1024));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 0, 1024));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 1, 1025));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 1, 1025));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamInlineClobDataTypeValuesWithNonClobDataTypeField() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 1000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 1000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (1, ?, ?, 'Test1')", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 1000));
        NClob nclob2 = createNClob(part(JSON_DATA2, 1, 1000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (2, ?, ?, 'Test2')", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 2, 1000));
        NClob nclob3 = createNClob(part(JSON_DATA2, 2, 1000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (3, ?, ?, 'Test3')", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Clob clob1Update = createClob(part(JSON_DATA, 1, 1000));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 1000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test1U' WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 0, 1024));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 0, 1024));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test2U' WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 1, 1025));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 1, 1025));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test3U' WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamLargeClobDataTypeValues() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (1, ?, ?)", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 24450));
        NClob nclob2 = createNClob(part(JSON_DATA2, 1, 24450));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (2, ?, ?)", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 3, 24450));
        NClob nclob3 = createNClob(part(JSON_DATA2, 3, 24450));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (3, ?, ?)", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));

        // Update record
        Clob clob1Update = createClob(part(JSON_DATA, 1, 24500));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 24500));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 2, 25000));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 2, 25000));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 3, 25000));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 3, 25000));
        connection.prepareQuery("UPDATE CLOB_TEST SET val_clob=?, val_nclob=? WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);
        records.forEach(System.out::println);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamLargeClobDataTypeValuesWithNonClobDataTypeField() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_DATA varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (1, ?, ?, 'Test1')", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 24450));
        NClob nclob2 = createNClob(part(JSON_DATA2, 2, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (2, ?, ?, 'Test2')", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 3, 24450));
        NClob nclob3 = createNClob(part(JSON_DATA2, 4, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (3, ?, ?, 'Test3')", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3");

        // Update record
        Clob clob1Update = createClob(part(JSON_DATA, 1, 24500));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 24500));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test1U' WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 2, 25000));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 2, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test2U' WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 3, 25000));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 3, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_data='Test3U' WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));
        assertThat(after.get("VAL_DATA")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_DATA")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamMixedClobDataTypeValuesWithNonClobFieldsSameTable() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_CLOBS clob, "
                + "VAL_NCLOBS nclob, "
                + "VAL_VARCHAR2 varchar2(50),"
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (1, ?, ?, 'ClobTest', 'NClobTest', 'Test1')", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 24450));
        NClob nclob2 = createNClob(part(JSON_DATA2, 2, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (2, ?, ?, 'ClobTest2', 'NClobTest2', 'Test2')", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 3, 24450));
        NClob nclob3 = createNClob(part(JSON_DATA2, 4, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (3, ?, ?, 'ClobTest3', 'NClobTest3', 'Test3')", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest2");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest2");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest3");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest3");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test3");

        // Update record
        Clob clob1Update = createClob(part(JSON_DATA, 1, 24500));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 24500));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test1U' WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
            ps.setString(3, "ClobTest1Updated");
            ps.setString(4, "NClobTest1Updated");
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest1Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest1Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 2, 25000));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 2, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test2U' WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
            ps.setString(3, "ClobTest2Updated");
            ps.setString(4, "NClobTest2Updated");
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 3, 25000));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 3, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test3U' WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
            ps.setString(3, "ClobTest3Updated");
            ps.setString(4, "NClobTest3Updated");
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest2Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest2Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest3Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest3Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldNotStreamAnyChangesWhenLobEraseIsDetected() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        LogInterceptor logInterceptor = new LogInterceptor();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        connection.prepareQuery("INSERT INTO CLOB_TEST VALUES (1, ?)", ps -> ps.setClob(1, clob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));

        // Execute LOB_ERASE
        connection.execute("DECLARE loc_c CLOB; amount integer; BEGIN " +
                "SELECT \"VAL_CLOB\" INTO loc_c FROM CLOB_TEST WHERE ID = 1 for update; " +
                "amount := 10;" +
                "dbms_lob.erase(loc_c, amount, 1); end;");

        // Wait until the log has recorded the message.
        Awaitility.await().atMost(Duration.ofMinutes(1)).until(() -> logInterceptor.containsWarnMessage("LOB_ERASE for table"));
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamClobDataTypeValuesWithPrimaryKeyChange() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_CLOBS clob, "
                + "VAL_NCLOBS nclob, "
                + "VAL_VARCHAR2 varchar2(50), "
                + "primary key(id))";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (1, ?, ?, 'ClobTest', 'NClobTest', 'Test1')", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1");

        // Update record, including primary key
        Clob clob1Update = createClob(part(JSON_DATA, 1, 24500));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 24500));
        connection.prepareQuery("UPDATE clob_test SET id=2, val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test1U' WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
            ps.setString(3, "ClobTest1Updated");
            ps.setString(4, "NClobTest1Updated");
        }, null);
        connection.commit();

        // 3 records, one indicating the deletion of PK 1, tombstone, and PK 2
        records = consumeRecordsByTopic(3);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(3);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest1Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest1Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1U");
    }

    @Test
    @FixFor("DBZ-2948")
    public void shouldStreamClobDataTypeValuesUsingBasicFileStorage() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_CLOBS clob, "
                + "VAL_NCLOBS nclob, "
                + "VAL_VARCHAR2 varchar2(50), "
                + "primary key(id)) "
                + "LOB(VAL_CLOB) STORE AS BASICFILE "
                + "LOB(VAL_NCLOB) STORE AS BASICFILE "
                + "LOB(VAL_CLOBS) STORE AS BASICFILE "
                + "LOB(VAL_NCLOBS) STORE AS BASICFILE";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.clob_test");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("INSERT INTO clob_test VALUES (1, ?, ?, 'ClobTest', 'NClobTest', 'Test1')", ps -> {
            ps.setClob(1, clob1);
            ps.setNClob(2, nclob1);
        }, null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1");

        // Insert multiple records, same transaction
        Clob clob2 = createClob(part(JSON_DATA, 1, 24450));
        NClob nclob2 = createNClob(part(JSON_DATA2, 2, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (2, ?, ?, 'ClobTest2', 'NClobTest2', 'Test2')", ps -> {
            ps.setClob(1, clob2);
            ps.setNClob(2, nclob2);
        }, null);
        Clob clob3 = createClob(part(JSON_DATA, 3, 24450));
        NClob nclob3 = createNClob(part(JSON_DATA2, 4, 24450));
        connection.prepareQuery("INSERT INTO clob_test VALUES (3, ?, ?, 'ClobTest3', 'NClobTest3', 'Test3')", ps -> {
            ps.setClob(1, clob3);
            ps.setNClob(2, nclob3);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest2");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest2");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test2");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidInsert(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest3");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest3");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test3");

        // Update record
        System.out.println("*** Start ***");
        Clob clob1Update = createClob(part(JSON_DATA, 1, 24500));
        NClob nclob1Update = createNClob(part(JSON_DATA2, 1, 24500));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test1U' WHERE id = 1", ps -> {
            ps.setClob(1, clob1Update);
            ps.setNClob(2, nclob1Update);
            ps.setString(3, "ClobTest1Updated");
            ps.setString(4, "NClobTest1Updated");
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest1Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest1Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test1U");

        // Update multiple records, same transaction
        Clob clob2Update = createClob(part(JSON_DATA, 2, 25000));
        NClob nclob2Update = createNClob(part(JSON_DATA2, 2, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test2U' WHERE id = 2", ps -> {
            ps.setClob(1, clob2Update);
            ps.setNClob(2, nclob2Update);
            ps.setString(3, "ClobTest2Updated");
            ps.setString(4, "NClobTest2Updated");
        }, null);
        Clob clob3Update = createClob(part(JSON_DATA, 3, 25000));
        NClob nclob3Update = createNClob(part(JSON_DATA2, 3, 25000));
        connection.prepareQuery("UPDATE clob_test SET val_clob=?, val_nclob=?, val_clobs=?, val_nclobs=?, val_varchar2='Test3U' WHERE id = 3", ps -> {
            ps.setClob(1, clob3Update);
            ps.setNClob(2, nclob3Update);
            ps.setString(3, "ClobTest3Updated");
            ps.setString(4, "NClobTest3Updated");
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 2);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob2Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob2Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest2Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest2Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test2U");

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(1);
        VerifyRecord.isValidUpdate(record, "ID", 3);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob3Update));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob3Update));
        assertThat(after.get("VAL_CLOBS")).isEqualTo("ClobTest3Updated");
        assertThat(after.get("VAL_NCLOBS")).isEqualTo("NClobTest3Updated");
        assertThat(after.get("VAL_VARCHAR2")).isEqualTo("Test3U");

        // Delete record
        connection.execute("DELETE FROM debezium.clob_test WHERE id = 1");

        // two records, delete + tombstone
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(2);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test1U");

        assertThat(after(record)).isNull();

        // Delete multiple records, same transaction
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 2");
        connection.executeWithoutCommitting("DELETE FROM debezium.clob_test WHERE id = 3");
        connection.execute("COMMIT");

        // 2 deletes + 2 tombstones
        records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(4);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidDelete(record, "ID", 2);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test2U");

        assertThat(after(record)).isNull();

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(2);
        VerifyRecord.isValidDelete(record, "ID", 3);

        // clob fields will never have a "before" state; emitted with unavailable value placeholder
        before = before(record);
        assertThat(before.get("ID")).isEqualTo(3);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_NCLOB")).isEqualTo(getUnavailableValuePlaceholder(config));
        assertThat(before.get("VAL_VARCHAR2")).isEqualTo("Test3U");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-3631")
    public void shouldReconcileTransactionWhenAllBlobClobAreInitializedAsNull() throws Exception {
        final String DDL = "CREATE TABLE dbz3631 ("
                + "ID NUMBER(38) NOT NULL,"
                + "ENTITY_ID NUMBER(38) NOT NULL,"
                + "DOCX CLOB,"
                + "DOCX_SIGNATURE CLOB,"
                + "XML_OOS CLOB,"
                + "XML_OOS_SIGNATURE CLOB,"
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
    public void shouldNotEmitClobFieldValuesWhenLobSupportIsNotEnabled() throws Exception {
        boolean logMinerAdapter = TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER);
        TestHelper.dropTable(connection, "dbz3645");
        try {
            connection.execute("CREATE TABLE dbz3645 (id numeric(9,0), data clob, primary key(id))");
            TestHelper.streamTable(connection, "dbz3645");

            // Small data
            connection.execute("INSERT INTO dbz3645 (id,data) values (1,'Test1')");

            // Large data
            Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (2,?)", ps -> ps.setClob(1, clob1), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3645")
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
            connection.executeWithoutCommitting("INSERT INTO dbz3645 (id,data) values (3,'Test3')");
            connection.prepareQuery("INSERT INTO dbz3645 (id,data) values (4,?)", ps -> ps.setClob(1, clob1), null);
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

            // LogMiner will pickup a separate update for CLOB fields.
            // There is no way to differentiate this change from any other UPDATE so the connector
            // will continue to emit it, but as a stand-alone UPDATE rather than merging it with
            // the parent INSERT as it would when LOB is enabled.
            if (logMinerAdapter) {
                record = table.get(1);
                after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(3);
                assertThat(after.get("DATA")).isEqualTo("Test3");
                assertThat(((Struct) record.value()).get("op")).isEqualTo("u");
            }

            // the second insert won't emit an update due to the clob field being set by using the
            // SELECT_LOB_LOCATOR, LOB_WRITE, and LOB_TRIM operators when using LogMiner and the
            // CLOB field will be excluded automatically by Xstream due to skipping chunk processing.
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
    public void shouldStreamNotNullClobUsingEmptyClobFunction() throws Exception {
        TestHelper.dropTable(connection, "dbz3898");
        try {
            connection.execute("CREATE TABLE dbz3898 (id numeric(9,0), data clob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz3898");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3898")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Empty function usage
            connection.execute("INSERT INTO dbz3898 (id,data) values (1,EMPTY_CLOB())");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ3898"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ3898")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("");

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3898");
        }
    }

    @Test
    @FixFor("DBZ-4276")
    public void shouldStreamClobWithUnavailableColumnValuePlaceholder() throws Exception {
        TestHelper.dropTable(connection, "dbz4276");
        try {
            connection.execute("CREATE TABLE dbz4276 (id numeric(9,0), data clob not null, data2 nclob not null, data3 varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4276");

            // Empty function usage
            connection.execute("INSERT INTO dbz4276 (id,data,data2,data3) values (1,EMPTY_CLOB(),EMPTY_CLOB(),'Test')");

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
            assertThat(after.get("DATA")).isEqualTo("");
            assertThat(after.get("DATA2")).isEqualTo("");
            assertThat(after.get("DATA3")).isEqualTo("Test");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Test unavailable column placeholder with update
            connection.execute("UPDATE dbz4276 set data3 = '123' WHERE id = 1");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4276"))).hasSize(1);

            record = records.recordsForTopic(topicName("DBZ4276")).get(0);
            Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("DATA")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
            assertThat(before.get("DATA2")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
            assertThat(before.get("DATA3")).isEqualTo("Test");

            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
            assertThat(after.get("DATA2")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
            assertThat(after.get("DATA3")).isEqualTo("123");

            // Test unavailable column placeholder with update
            connection.execute("DELETE FROM dbz4276 WHERE id = 1");

            records = consumeRecordsByTopic(2); // delete and tombstone
            assertThat(records.recordsForTopic(topicName("DBZ4276"))).hasSize(2);

            record = records.recordsForTopic(topicName("DBZ4276")).get(0);
            before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("DATA")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
            assertThat(before.get("DATA2")).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
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

    private Clob createClob(String data) throws SQLException {
        Clob clob = connection.connection().createClob();
        clob.setString(1, data);
        return clob;
    }

    private NClob createNClob(String data) throws SQLException {
        NClob nclob = connection.connection().createNClob();
        nclob.setString(1, data);
        return nclob;
    }

    private static String part(String text, int start, int length) {
        return text == null ? "" : text.substring(start, Math.min(length, text.length()));
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

    private static String getClobString(Clob clob) throws SQLException {
        return clob.getSubString(1, (int) clob.length());
    }

    private static String getUnavailableValuePlaceholder(Configuration config) {
        return config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER);
    }
}
