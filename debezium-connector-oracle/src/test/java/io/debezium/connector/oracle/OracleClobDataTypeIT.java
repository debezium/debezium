/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
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
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer;
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
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
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
    @FixFor({ "DBZ-2948", "DBZ-5773" })
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "OpenLogReplicator does not differentiate between LOB operations")
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

        LogInterceptor logminerLogInterceptor = new LogInterceptor(TransactionCommitConsumer.class);
        final LogInterceptor xstreamLogInterceptor = new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");
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
        Awaitility.await().atMost(Duration.ofMinutes(1))
                .until(() -> logminerLogInterceptor.containsWarnMessage("LOB_ERASE for table")
                        || xstreamLogInterceptor.containsWarnMessage("LOB_ERASE for table"));
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor({ "DBZ-2948", "DBZ-5773" })
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.OLR, reason = "OpenLogReplicator does not differentiate between LOB operations")
    public void shouldStreamChangesWhenLobEraseIsDetected() throws Exception {
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

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert record
        Clob clob1 = createClob(part(JSON_DATA, 0, 24000));
        connection.prepareQuery("INSERT INTO debezium.clob_test values (1, ?)", p -> p.setClob(1, clob1), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));

        // Execute LOB_ERASE
        connection.execute("DECLARE loc_c CLOB; amount integer; BEGIN "
                + "SELECT \"VAL_CLOB\" INTO loc_c FROM CLOB_TEST WHERE ID = 1 for update; "
                + "amount := 10;"
                + "dbms_lob.erase(loc_c, amount, 1); end;");

        // Wait until the log has recorded the message.
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        Struct before = before(record);
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getUnavailableValuePlaceholder(config));

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
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

            record = table.get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));

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
            if (logMinerAdapter) {
                // With LogMiner, the first event only contains the initialization of id
                assertThat(after.get("DATA")).isNull();
            }
            else {
                // Xstream combines the insert and subsequent LogMiner update into a single insert event
                // automatically, so we receive the value here where the LogMiner implementation doesn't.
                assertThat(after.get("DATA")).isEqualTo("Test3");
            }
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
                assertThat(after.get("DATA")).isEqualTo(getClobString(clob1));
            }
            assertThat(((Struct) record.value()).get("op")).isEqualTo("c");

            // Test updates with small clob fields
            connection.executeWithoutCommitting("UPDATE dbz3645 set data='Test3U' WHERE id = 3");
            connection.commit();

            // Get streaming records
            sourceRecords = consumeRecordsByTopic(1);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidUpdate(table.get(0), "ID", 3);

            // When updating a table that contains a small CLOB value but the update does not modify
            // any of the non-CLOB fields, we expect the placeholder in the before and the value in the after.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(getAfterField(table.get(0), "DATA")).isEqualTo("Test3U");
            assertNoRecordsToConsume();

            // Test updates with large clob fields
            Clob clob2 = createClob(part(JSON_DATA, 1000, 21500));
            connection.prepareQuery("UPDATE dbz3645 set data=? WHERE id=4", ps -> ps.setClob(1, clob2), null);
            connection.commit();

            if (logMinerAdapter) {
                // When updating a table that contains a large CLOB value but the update does not modify
                // any of the non-CLOB fields, don't expect any events to be emitted. This is because
                // the event is treated as a SELECT_LOB_LOCATOR and LOB_WRITE series which is ignored.
                waitForAvailableRecords(10, TimeUnit.SECONDS);
            }
            else {
                // Xstream actually picks up this particular event.
                sourceRecords = consumeRecordsByTopic(1);
                table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
                VerifyRecord.isValidUpdate(table.get(0), "ID", 4);
                assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getAfterField(table.get(0), "DATA")).isEqualTo(getClobString(clob2));
            }

            assertNoRecordsToConsume();

            // Update updates small clob row by changing non-clob fields
            connection.executeWithoutCommitting("UPDATE dbz3645 set id=5 where id=3");
            connection.commit();

            // Get streaming records
            // We expect 3 events: delete for ID=3, tombstone for ID=3, and insert for ID=5
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 3);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 3);
            VerifyRecord.isValidInsert(table.get(2), "ID", 5);

            // When updating a table that contains a CLOB value but the update does not modify
            // any of the CLOB fields, we expect the placeholder.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test update large clob row by changing non-clob fields
            connection.executeWithoutCommitting("UPDATE dbz3645 SET ID=6 WHERE ID=4");
            connection.commit();

            // Get streaming records
            // We expect 3 events: delete for ID=4, tombstone for ID=4, and insert for ID=6
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 4);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 4);
            VerifyRecord.isValidInsert(table.get(2), "ID", 6);

            // When updating a table that contains a large CLOB value but the update does not modify
            // any of the CLOB fields, we expect the placeholder.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test updating both small clob and non-clob fields
            Clob clob1u2 = createClob(part(JSON_DATA, 10, 260));
            connection.prepareQuery("UPDATE dbz3645 SET data=?, id=7 WHERE id=5", ps -> ps.setClob(1, clob1u2), null);
            connection.commit();

            // Get streaming records
            // The number of expected records depends on whether this test is using LogMiner or Xstream.
            // LogMiner expects 4: delete for ID=5, tombstone for ID=5, create for ID=7, update for ID=7
            // XStream expects 3: delete for ID=5, tombstone for ID=5, create for ID=7
            //
            // NOTE: The extra update event is because the CLOB value is treated inline and so LogMiner
            // does not emit a SELECT_LOB_LOCATOR event but rather a subsequent update that is captured
            // but not merged since event merging happens only when LOB is enabled. Xstream handles
            // this automatically hence the reason why it has 1 less event in the stream.
            sourceRecords = consumeRecordsByTopic(logMinerAdapter ? 4 : 3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 5);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 5);
            VerifyRecord.isValidInsert(table.get(2), "ID", 7);

            if (logMinerAdapter) {
                VerifyRecord.isValidUpdate(table.get(3), "ID", 7);
            }

            // When updating a table's small clob and non-clob columns
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            if (logMinerAdapter) {
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getBeforeField(table.get(3), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
                assertThat(getAfterField(table.get(3), "DATA")).isEqualTo(getClobString(clob1u2));
            }
            else {
                // Xstream combines the insert/update into a single insert
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getClobString(clob1u2));
            }
            assertNoRecordsToConsume();

            // Test updating both large clob and non-clob fields
            Clob clob2u2 = createClob(part(JSON_DATA, 10, 12500));
            connection.prepareQuery("UPDATE dbz3645 SET data=?, id=8 WHERE id=6", ps -> ps.setClob(1, clob2u2), null);
            connection.commit();

            // Get streaming records
            // Expect 3 records: delete for ID=6, tombstone for ID=6, create for ID=8
            sourceRecords = consumeRecordsByTopic(3);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 6);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 6);
            VerifyRecord.isValidInsert(table.get(2), "ID", 8);

            // When updating a table's large clob and non-clob columns, we expect placeholder in before
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            if (logMinerAdapter) {
                // LogMiner is unable to provide the value, so it gets emitted with the placeholder.
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            }
            else {
                // Xstream gets the value, so its provided.
                assertThat(getAfterField(table.get(2), "DATA")).isEqualTo(getClobString(clob2u2));
            }
            assertNoRecordsToConsume();

            if (!logMinerAdapter) {
                return;
            }

            // delete row with small clob data
            connection.execute("DELETE FROM dbz3645 WHERE id=7");

            // Get streaming records
            // We expect 2 events: delete for ID=7, tombstone for ID=7
            sourceRecords = consumeRecordsByTopic(2);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 7);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 7);

            // When delete from a table that contains a CLOB value we always expect the placeholder
            // to be supplied, even when LOB support is disabled.
            assertThat(getBeforeField(table.get(0), "DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertNoRecordsToConsume();

            // Test deleting a row from a table with a large clob column
            connection.execute("DELETE FROM dbz3645 WHERE id=8");

            // Get streaming records
            // We expect 2 events: delete for ID=8, tombstone for ID=8
            sourceRecords = consumeRecordsByTopic(2);
            table = sourceRecords.recordsForTopic(topicName("DBZ3645"));
            VerifyRecord.isValidDelete(table.get(0), "ID", 8);
            VerifyRecord.isValidTombstone(table.get(1), "ID", 8);

            // When delete from a table that contains a CLOB value we always expect the placeholder
            // to be supplied, even when LOB support is disabled.
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

    @Test
    @FixFor("DBZ-4366")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Xstream marks chunks as end of rows")
    public void shouldStreamClobsWrittenInChunkedMode() throws Exception {
        TestHelper.dropTable(connection, "dbz4366");
        try {
            connection.execute("CREATE TABLE dbz4366 (id numeric(9,0), val_clob clob not null, val_nclob nclob not null, primary key(id))");
            TestHelper.streamTable(connection, "dbz4366");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4366")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz4366 (id,val_clob,val_nclob) values (1,EMPTY_CLOB(),EMPTY_CLOB())");
            // for bonus points, interleave the writes to the LOB fields
            final String fillQuery = "DECLARE\n" +
                    "  loc CLOB;\n" +
                    "  nloc NCLOB;\n" +
                    "  i PLS_INTEGER;\n" +
                    "  str VARCHAR2(1024);\n" +
                    "BEGIN\n" +
                    "  str := ?;\n" +
                    "  SELECT val_clob into loc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  SELECT val_nclob into nloc FROM dbz4366 WHERE id = 1 FOR UPDATE;\n" +
                    "  DBMS_LOB.OPEN(loc, DBMS_LOB.LOB_READWRITE);\n" +
                    "  DBMS_LOB.OPEN(nloc, DBMS_LOB.LOB_READWRITE);\n" +
                    "  FOR i IN 1..1024 LOOP\n" +
                    "    DBMS_LOB.WRITEAPPEND(loc, 1024, str);\n" +
                    "    DBMS_LOB.WRITEAPPEND(nloc, 1024, str);\n" +
                    "  END LOOP;\n" +
                    "  DBMS_LOB.CLOSE(loc);\n" +
                    "  DBMS_LOB.CLOSE(nloc);\n" +
                    "END;";
            connection.prepareQuery(fillQuery, ps -> ps.setString(1, part(JSON_DATA, 0, 1024)), null);
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DBZ4366"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DBZ4366")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            String clobval = (String) after.get("VAL_CLOB");
            assertThat(clobval.length()).isEqualTo(1024 * 1024);
            String nclobval = (String) after.get("VAL_NCLOB");
            assertThat(nclobval.length()).isEqualTo(1024 * 1024);

            // As a sanity check, there should be no more records.
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4366");
        }
    }

    @Test
    @FixFor({ "DBZ-4891", "DBZ-4862", "DBZ-4994" })
    public void shouldStreamClobValueWithEscapedSingleQuoteValue() throws Exception {
        String ddl = "CREATE TABLE CLOB_TEST ("
                + "ID numeric(9,0), "
                + "VAL_CLOB clob, "
                + "VAL_NCLOB nclob, "
                + "VAL_USERNAME varchar2(100),"
                + "VAL_DATA varchar2(100), "
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

        // Create simple insert, will be used for updates later
        final String simpleQuote = "This will be fixed soon so please don''t worry, she wrote.";
        final String complexQuote = "2\"''\" sd f\"\"\" '''''''' ''''";
        connection.execute("INSERT INTO clob_test (id,val_username,val_data) values (1,'" + simpleQuote + "','" + complexQuote + "')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        SourceRecord record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        // Update the record this way to enforce that both varchar fields are present in the SELECT_LOB_LOCATOR
        // event that will need to be parsed by the SelectLobParser component.
        Clob clob1 = createClob(part(JSON_DATA, 0, 25000));
        NClob nclob1 = createNClob(part(JSON_DATA2, 0, 25000));
        connection.prepareQuery("update clob_test set val_clob=?, val_nclob=? where id=1", ps -> {
            ps.setClob(1, clob1);
            ps.setClob(2, nclob1);
        }, null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("CLOB_TEST"))).hasSize(1);

        record = records.recordsForTopic(topicName("CLOB_TEST")).get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        // Validate update data
        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VAL_CLOB")).isEqualTo(getClobString(clob1));
        assertThat(after.get("VAL_NCLOB")).isEqualTo(getClobString(nclob1));
        assertThat(after.get("VAL_USERNAME")).isEqualTo("This will be fixed soon so please don't worry, she wrote.");
        assertThat(after.get("VAL_DATA")).isEqualTo("2\"'\" sd f\"\"\" '''' ''");
    }

    @Test
    @FixFor("DBZ-5266")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Commit SCN is only applicable to LogMiner")
    public void shouldUpdateCommitScnOnLobTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz5266");
        try {
            connection.execute("create table dbz5266 (data clob)");
            TestHelper.streamTable(connection, "dbz5266");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5266")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String query = "INSERT INTO dbz5266 values (?)";
            try (PreparedStatement ps = connection.connection().prepareStatement(query)) {
                final URL resource = getClass().getClassLoader().getResource("data/test_lob_data.json");
                final File file = new File(resource.toURI());
                ps.setCharacterStream(1, new FileReader(file), file.length());
                ps.addBatch();
                ps.executeBatch();
                connection.commit();
            }
            catch (Exception e) {
                fail("Insert of clob data failed to happen", e);
            }

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5266");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("DATA")).isNotNull();

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct("source");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.COMMIT_SCN_KEY)).isNotNull();

            final String commitScn = source.getString(SourceInfo.COMMIT_SCN_KEY);
            final String scn = source.getString(SourceInfo.SCN_KEY);
            assertThat(Scn.valueOf(commitScn).longValue()).isGreaterThanOrEqualTo(Scn.valueOf(scn).longValue());
        }
        finally {
            TestHelper.dropTable(connection, "dbz5266");
        }
    }

    @Test
    @FixFor("DBZ-5266")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Commit SCN is only applicable to LogMiner")
    public void shouldUpdateCommitScnOnNonLobTransactionWithLobEnabled() throws Exception {
        TestHelper.dropTable(connection, "dbz5266");
        try {
            connection.execute("create table dbz5266 (data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5266");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5266")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz5266 values ('test')");

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5266");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("DATA")).isNotNull();

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct("source");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.COMMIT_SCN_KEY)).isNotNull();

            final String commitScn = source.getString(SourceInfo.COMMIT_SCN_KEY);
            final String scn = source.getString(SourceInfo.SCN_KEY);
            assertThat(Scn.valueOf(commitScn).longValue()).isGreaterThanOrEqualTo(Scn.valueOf(scn).longValue());
        }
        finally {
            TestHelper.dropTable(connection, "dbz5266");
        }
    }

    @Test
    @FixFor("DBZ-5266")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Commit SCN is only applicable to LogMiner")
    public void shouldUpdateCommitScnOnNonLobTransactionWithLobDisabled() throws Exception {
        TestHelper.dropTable(connection, "dbz5266");
        try {
            connection.execute("create table dbz5266 (data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5266");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5266")
                    .with(OracleConnectorConfig.LOB_ENABLED, false)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz5266 values ('test')");

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5266");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("DATA")).isNotNull();

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct("source");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.COMMIT_SCN_KEY)).isNotNull();

            final String commitScn = source.getString(SourceInfo.COMMIT_SCN_KEY);
            final String scn = source.getString(SourceInfo.SCN_KEY);
            assertThat(Scn.valueOf(commitScn).longValue()).isGreaterThanOrEqualTo(Scn.valueOf(scn).longValue());
        }
        finally {
            TestHelper.dropTable(connection, "dbz5266");
        }
    }

    @Test
    @FixFor("DBZ-5295")
    public void shouldReselectClobAfterPrimaryKeyChange() throws Exception {
        TestHelper.dropTable(connection, "dbz5295");
        try {
            connection.execute("create table dbz5295 (id numeric(9,0) primary key, data clob, data2 clob)");
            TestHelper.streamTable(connection, "dbz5295");

            connection.execute("INSERT INTO dbz5295 (id,data,data2) values (1,'Small clob data','Data2')");

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
            assertThat(after.get("DATA")).isEqualTo("Small clob data");
            assertThat(after.get("DATA2")).isEqualTo("Data2");

            connection.execute("UPDATE dbz5295 set id = 2 where id = 1");

            // The update of the primary key causes a DELETE and a CREATE, mingled with a TOMBSTONE
            records = consumeRecordsByTopic(3);
            recordsForTopic = records.recordsForTopic(topicName("DBZ5295"));
            assertThat(recordsForTopic).hasSize(3);

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
            assertThat(after.get("DATA")).isEqualTo("Small clob data");
            assertThat(after.get("DATA2")).isEqualTo("Data2");
        }
        finally {
            TestHelper.dropTable(connection, "dbz5295");
        }
    }

    @Test
    @FixFor("DBZ-5295")
    public void shouldReselectClobAfterPrimaryKeyChangeWithRowDeletion() throws Exception {
        TestHelper.dropTable(connection, "dbz5295");
        try {
            connection.execute("create table dbz5295 (id numeric(9,0) primary key, data clob, data2 clob)");
            TestHelper.streamTable(connection, "dbz5295");

            connection.execute("INSERT INTO dbz5295 (id,data,data2) values (1,'Small clob data','Data2')");

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
            assertThat(after.get("DATA")).isEqualTo("Small clob data");
            assertThat(after.get("DATA2")).isEqualTo("Data2");

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
            assertThat(after.get("DATA2")).isEqualTo(getUnavailableValuePlaceholder(config));

            // Fourth event: DELETE
            record = recordsForTopic.get(3);
            VerifyRecord.isValidDelete(record, "ID", 2);
            Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.get("DATA")).isEqualTo(getUnavailableValuePlaceholder(config));
            assertThat(before.get("DATA2")).isEqualTo(getUnavailableValuePlaceholder(config));
        }
        finally {
            TestHelper.dropTable(connection, "dbz5295");
        }
    }

    @Test
    @FixFor("DBZ-5581")
    public void testClobUnavailableValuePlaceholderUpdateOnlyOneClobColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz5581");
        try {
            connection.execute("create table dbz5581 (id numeric(9,0) primary key, a1 varchar2(200), a2 clob, a3 nclob, a4 varchar2(100))");
            TestHelper.streamTable(connection, "dbz5581");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5581")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final Clob a2 = createClob(part(JSON_DATA, 0, 4100));
            final NClob a3 = createNClob(part(JSON_DATA2, 0, 4100));
            connection.prepareQuery("INSERT into dbz5581 (id,a1,a2,a3,a4) values (1, 'lwmzVQd6r7', ?, ?, 'cuTVQV0OpK')", st -> {
                st.setClob(1, a2);
                st.setNClob(2, a3);
            }, null);
            connection.commit();

            final Clob a2u = createClob(part(JSON_DATA, 1, 4101));
            connection.prepareQuery("UPDATE dbz5581 set A2=? WHERE ID=1", st -> st.setClob(1, a2u), null);
            connection.commit();

            connection.execute("UPDATE dbz5581 set A2=NULL WHERE ID=1");

            SourceRecords records = consumeRecordsByTopic(3);
            List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DBZ5581");
            assertThat(recordsForTopic).hasSize(3);

            SourceRecord record = recordsForTopic.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A1")).isEqualTo("lwmzVQd6r7");
            assertThat(after.get("A2")).isEqualTo(getClobString(a2));
            assertThat(after.get("A3")).isEqualTo(getClobString(a3));
            assertThat(after.get("A4")).isEqualTo("cuTVQV0OpK");

            record = recordsForTopic.get(1);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A1")).isEqualTo("lwmzVQd6r7");
            assertThat(after.get("A2")).isEqualTo(getClobString(a2u));
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
    @FixFor("DBZ-7006")
    public void shouldStreamClobDataDataThatContainsSingleQuotesAtSpecificBoundaries() throws Exception {
        TestHelper.dropTable(connection, "dbz7006");
        try {
            // Test data
            final String insertData = replaceCharAt(createRandomStringWithAlphaNumeric(2000), 999, '\'');
            final String updateData = replaceCharAt(createRandomStringWithAlphaNumeric(2000), 999, '\'');

            connection.execute("CREATE TABLE dbz7006 (id numeric(9,0) primary key, data clob)");
            TestHelper.streamTable(connection, "dbz7006");

            final Clob snapshotClob = createClob(insertData);
            connection.prepareQuery("INSERT INTO dbz7006 values (1,?)", ps -> ps.setClob(1, snapshotClob), null);
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7006")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert - streaming
            final Clob insertClob = createClob(insertData);
            connection.prepareQuery("INSERT INTO dbz7006 values (2,?)", ps -> ps.setClob(1, insertClob), null);
            connection.commit();

            // Update - streaming
            final Clob updateClob = createClob(updateData);
            connection.prepareQuery("UPDATE dbz7006 set data = ? WHERE id = 2", ps -> ps.setClob(1, updateClob), null);
            connection.commit();

            final SourceRecords records = consumeRecordsByTopic(3);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ7006"));

            // Snapshot
            SourceRecord snapshot = tableRecords.get(0);
            VerifyRecord.isValidRead(snapshot, "ID", (byte) 1);
            assertThat(getAfterField(snapshot, "DATA")).isEqualTo(insertData);

            // Streaming
            SourceRecord insert = tableRecords.get(1);
            VerifyRecord.isValidInsert(insert, "ID", (byte) 2);
            assertThat(getAfterField(insert, "DATA")).isEqualTo(insertData);

            SourceRecord update = tableRecords.get(2);
            VerifyRecord.isValidUpdate(update, "ID", (byte) 2);
            assertThat(getAfterField(update, "DATA")).isEqualTo(updateData);

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz7006");
        }
    }

    private String createRandomStringWithAlphaNumeric(int length) {
        return RandomStringUtils.randomAlphabetic(length);
    }

    private String replaceCharAt(String data, int index, char ch) {
        StringBuilder sb = new StringBuilder(data);
        sb.setCharAt(index, ch);
        return sb.toString();
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

    private static Object getBeforeField(SourceRecord record, String fieldName) {
        return before(record).get(fieldName);
    }

    private static Object getAfterField(SourceRecord record, String fieldName) {
        return after(record).get(fieldName);
    }
}
