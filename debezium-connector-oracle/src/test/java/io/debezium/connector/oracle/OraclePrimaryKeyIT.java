/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

/**
 * A set of integration tests that check operations on primary keys.
 *
 * @author Chris Cranford
 */
public class OraclePrimaryKeyIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();

        if (connection != null && connection.isConnected()) {
            connection.close();
        }
    }

    @Test
    @FixFor("dbz#7")
    public void testSchemaRecoveryPrimaryKeyIsNotNull() throws Exception {
        connection.execute("create table dbz7 (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz7");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Verify JDBC metadata read sets primary key optionality correctly
        connection.execute("insert into dbz7 values (1, '1')");
        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("DATA", Schema.OPTIONAL_STRING_SCHEMA, "1"));

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);

        stopConnector();

        config = config.edit().with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY).build();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Verify schema history recovery sets primary key optionality correctly when
        // the DDL parser behavior is disabled (the default)
        connection.execute("insert into dbz7 values (2, '2')");
        expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 2),
                new SchemaAndValueField("DATA", Schema.OPTIONAL_STRING_SCHEMA, "2"));

        records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);

        stopConnector();

        config = config.edit().with(SchemaHistory.INTERNAL_PREFER_DDL, "true").build();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Verify schema history recovery sets primary key optionality correctly when
        // the DDL parser behavior is enabled (goes through ANTLR)
        connection.execute("insert into dbz7 values (3, '3')");
        expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 3),
                new SchemaAndValueField("DATA", Schema.OPTIONAL_STRING_SCHEMA, "3"));

        records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);
    }

    @Test
    @FixFor("dbz#7")
    public void testAlterTableAddPrimaryKeyIsNotNull() throws Exception {
        connection.execute("create table dbz7 (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz7");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Verify JDBC metadata read sets primary key optionality correctly
        connection.execute("insert into dbz7 values (1, '1')");
        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("DATA", Schema.OPTIONAL_STRING_SCHEMA, "1"));

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);

        // Verify old table handles primary key optionality via ANTLR parser
        connection.execute("ALTER TABLE dbz7 DROP PRIMARY KEY");
        connection.execute("ALTER TABLE dbz7 ADD PRIMARY KEY(id, data)");

        connection.execute("insert into dbz7 values (2, '2')");
        expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 2),
                new SchemaAndValueField("DATA", Schema.STRING_SCHEMA, "2"));

        records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);
    }

    @Test
    @FixFor("dbz#7")
    public void testCreateTableDuringStreamingPrimaryKeyIsNotNull() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // A table created during the streaming phase should have its primary key optionality
        // assigned through the ANTLR parser correctly.
        connection.execute("CREATE TABLE dbz7 (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz7");

        connection.execute("insert into dbz7 values (1, '1')");
        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("DATA", Schema.OPTIONAL_STRING_SCHEMA, "1"));

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);

        // Verify newly streamed table handles primary key optionality via ANTLR parser
        connection.execute("ALTER TABLE dbz7 DROP PRIMARY KEY");
        connection.execute("ALTER TABLE dbz7 ADD PRIMARY KEY(id, data)");

        connection.execute("insert into dbz7 values (2, '2')");
        expected = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 2),
                new SchemaAndValueField("DATA", Schema.STRING_SCHEMA, "2"));

        records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ7");
        assertThat(records).hasSize(1);
        assertRecordAfter(records.get(0), expected);
    }

    private static void assertRecordAfter(SourceRecord record, List<SchemaAndValueField> expected) {
        final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(after));
    }
}
