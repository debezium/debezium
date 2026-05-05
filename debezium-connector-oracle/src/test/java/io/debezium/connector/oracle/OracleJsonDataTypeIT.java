/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

import oracle.jdbc.OracleType;

/**
 * Integration tests for JSON data type support.
 *
 * <p>For Oracle LogMiner, the {@code JSON} data type is not emitted out-of-the-box. This data type
 * requires that the Oracle GoldenGate feature be enabled, otherwise LogMiner emits events for a table
 * that uses {@code JSON} as unsupported.
 *
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 21, minor = 0, reason = "JSON was added in Oracle 21+")
public class OracleJsonDataTypeIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        TestHelper.dropAllTables();

        if (TestHelper.isAnyLogMiner()) {
            TestHelper.enableGoldenGateReplication();
        }
    }

    @AfterAll
    static void afterAll() throws SQLException {
        if (TestHelper.isAnyLogMiner()) {
            TestHelper.disableGoldenGateReplication();
        }
    }

    @BeforeEach
    void beforeEach() {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "dbz1884");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz1884");
            connection.close();
        }
    }

    @Test
    @FixFor("dbz#1884")
    public void shouldSnapshotJsonDataTypeValues() throws Exception {
        connection.execute("CREATE TABLE dbz1884 (id number(9,0) primary key, extra_data json)");
        TestHelper.streamTable(connection, "dbz1884");

        final String jsonData = "{\"name\":\"Sally Field\",\"age\":77}";

        connection.prepareQuery("INSERT INTO dbz1884 (id,extra_data) values (1,?)",
                ps -> ps.setObject(1, jsonData, OracleType.JSON), null);
        connection.commit();

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ1884")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ1884"));
        assertThat(records).hasSize(1);

        final SourceRecord record = records.get(0);
        VerifyRecord.isValidRead(record, "ID", 1);
        assertThat(after(record).get("ID")).isEqualTo(1);
        assertThat(after(record).get("EXTRA_DATA")).isEqualTo(jsonData);
    }

    @Test
    @FixFor("dbz#1884")
    public void shouldStreamJsonDataTypeValuesWithoutLobEnabled() throws Exception {
        connection.execute("CREATE TABLE dbz1884 (id number(9,0) primary key, extra_data json)");
        TestHelper.streamTable(connection, "dbz1884");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ1884")
                .with(OracleConnectorConfig.LOB_ENABLED, Boolean.TRUE)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        final String jsonInsertData = "{\"name\":\"Sally Field\",\"age\":77}";
        final String jsonUpdateData = "{\"greet\":\"Hello World\"}";

        connection.prepareQuery("INSERT INTO dbz1884 (id,extra_data) values (1,?)",
                ps -> ps.setObject(1, jsonInsertData, OracleType.JSON), null);
        connection.commit();

        connection.execute("UPDATE dbz1884 SET extra_data = '%s' WHERE ID = 1".formatted(jsonUpdateData));
        connection.execute("DELETE FROM dbz1884 WHERE id = 1");

        List<SourceRecord> records = consumeRecordsByTopic(3).recordsForTopic(topicName("DBZ1884"));
        assertThat(records).hasSize(3);

        final SourceRecord insert = records.get(0);
        VerifyRecord.isValidInsert(insert, "ID", 1);
        assertThat(before(insert)).isNull();
        assertThat(after(insert).get("ID")).isEqualTo(1);
        assertThat(after(insert).get("EXTRA_DATA")).isEqualTo(jsonInsertData);

        final SourceRecord update = records.get(1);
        VerifyRecord.isValidUpdate(update, "ID", 1);
        assertThat(before(update).get("ID")).isEqualTo(1);
        assertThat(before(update).get("EXTRA_DATA")).isEqualTo(unavailableValuePlaceholder(config));
        assertThat(after(update).get("ID")).isEqualTo(1);
        assertThat(after(update).get("EXTRA_DATA")).isEqualTo(jsonUpdateData);

        final SourceRecord delete = records.get(2);
        VerifyRecord.isValidDelete(delete, "ID", 1);
        assertThat(before(delete).get("ID")).isEqualTo(1);
        assertThat(before(delete).get("EXTRA_DATA")).isEqualTo(unavailableValuePlaceholder(config));
        assertThat(after(records.get(2))).isNull();
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

    private static String unavailableValuePlaceholder(Configuration config) {
        return config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER);
    }
}
