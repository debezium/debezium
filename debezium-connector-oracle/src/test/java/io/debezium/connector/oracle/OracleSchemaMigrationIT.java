/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.antlr.listener.AlterTableParserListener;
import io.debezium.connector.oracle.antlr.listener.CreateTableParserListener;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

/**
 * Integration tests for the Oracle DDL and schema migration.
 *
 * @author Chris Cranford
 */
public class OracleSchemaMigrationIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();
    }

    @After
    public void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropAllTables();
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamNewlyCreatedNotFilteredTable() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        createTable("debezium.tableb", "CREATE TABLE debezium.tableb (ID numeric(9,0) primary key, data varchar2(50))");

        // Streaming should have generated 2 schema change event for tableb, create & alter
        // Connector does not apply any filters, so tableb should be included
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(2);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEB");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEB");

        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(1);
        assertStreamingSchemaChange(record);

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEB");

        // Insert record into tableb and verify it is streamed
        connection.execute("INSERT INTO debezium.tableb (ID,DATA) values (1, 'Test')");

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEB"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEB")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEB");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldNotStreamNewlyCreatedTableDueToFilters() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLEA")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        createTable("debezium.tableb", "CREATE TABLE debezium.tableb (ID numeric(9,0) primary key, data varchar2(50))");

        // While tableb is created during streaming, it should not generate any schema changes
        // because the filters explicitly only capture changes for tablea, not tableb.
        assertNoRecordsToConsume();

        // Insert a record into tableb
        // This record won't be captured due to the configuration filters
        connection.execute("INSERT INTO debezium.tableb (ID,DATA) values (1, 'B')");
        assertNoRecordsToConsume();

        // Insert a record into tablea
        // This record should be captured as this table is included in the filters
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'A')");

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertThat(((Struct) record.value()).getStruct("after").get("DATA")).isEqualTo("A");
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableAddColumnSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and add a single column using single column syntax
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea ADD data2 numeric");
        connection.execute("INSERT INTO debezium.tablea (ID,DATA,DATA2) values (2, 'Test2', 100)");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(3);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA")).isEqualTo("Test2");
        assertThat(after.get("DATA2")).isEqualTo(BigDecimal.valueOf(100));
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableAddMultipleColumnsSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and add multiple columns using multi-column syntax
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea ADD (data2 numeric, data3 varchar2(25))");
        connection.execute("INSERT INTO debezium.tablea (ID,DATA,DATA2,DATA3) values (2, 'Test2', 100, 'a')");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(4);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA")).isEqualTo("Test2");
        assertThat(after.get("DATA2")).isEqualTo(BigDecimal.valueOf(100));
        assertThat(after.get("DATA3")).isEqualTo("a");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableRenameColumnSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and rename column
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea RENAME COLUMN data TO data1");
        connection.execute("INSERT INTO debezium.tablea (ID,DATA1) values (2, 'Test2')");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.schema().field("DATA")).isNull();
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA1")).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableDropColumnSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and drop column
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea DROP COLUMN data");
        connection.execute("INSERT INTO debezium.tablea (ID) values (2)");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(1);
        assertThat(after.schema().field("DATA")).isNull();
        assertThat(after.get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableDropMultipleColumnsSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data1 varchar2(50), data2 numeric)");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA1,DATA2) values (1, 'Test', 100)");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(3);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA1")).isEqualTo("Test");
        assertThat(after.get("DATA2")).isEqualTo(BigDecimal.valueOf(100));

        // Alter tablea and drop multiple columns
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea DROP (data1, data2)");
        connection.execute("INSERT INTO debezium.tablea (ID) values (2)");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(1);
        assertThat(after.schema().field("DATA1")).isNull();
        assertThat(after.schema().field("DATA2")).isNull();
        assertThat(after.get("ID")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableRenameTableSchemaChange() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and rename to tableb
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea RENAME TO tableb");
        connection.execute("INSERT INTO debezium.tableb (ID,DATA) values (2, 'Test2')");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEB"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA,TABLEB");
        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEB");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEB")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEB");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA")).isEqualTo("Test2");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldNotStreamAfterTableRenameToExcludedName() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLEA")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Alter tablea and rename to tableb
        // Then insert a new record immediately afterward into tablea
        connection.executeWithoutCommitting("ALTER TABLE debezium.tablea RENAME TO tableb");
        connection.execute("INSERT INTO debezium.tableb (ID,DATA) values (2, 'Test2')");

        // There should be 1 records generated, 1 schema change and
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);

        // Verify schema change contains no changes
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA,TABLEB");
        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).isEmpty();

        // There should be no other events to consume
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableChangeColumnDataType() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data numeric)");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Alter table, change data from numeric to varchar2(50) then insert new record
        connection.execute("ALTER TABLE debezium.tablea modify (data varchar2(50))");
        connection.execute("INSERT INTO debezium.tablea (id, data) values (1, 'Test')");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableChangeColumnNullability() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50) not null)");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID,DATA) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        Schema dataSchema = after.schema().field("DATA").schema();
        assertThat(dataSchema.isOptional()).isFalse();

        // Alter table, change data from not null to null
        connection.execute("ALTER TABLE debezium.tablea modify (data varchar2(50) null)");
        connection.execute("INSERT INTO debezium.tablea (id) values (2)");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA")).isNull();

        dataSchema = after.schema().field("DATA").schema();
        assertThat(dataSchema.isOptional()).isTrue();
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamAlterTableChangeColumnPrecisionAndScale() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data numeric(8,2) not null)");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (ID, DATA) values (1, 12345.67)");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo(BigDecimal.valueOf(12345.67));

        // Verify column schema definition
        Schema dataSchema = after.schema().field("DATA").schema();
        assertThat(dataSchema.parameters().get("scale")).isEqualTo("2");
        assertThat(dataSchema.parameters().get("connect.decimal.precision")).isEqualTo("8");

        // Alter table, change data from not null to null
        connection.execute("ALTER TABLE debezium.tablea modify (data numeric(10,3))");
        connection.execute("INSERT INTO debezium.tablea (id, data) values (2, 234567.891)");

        // There should be 2 records generated, 1 schema change and 1 insert
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);

        // Verify schema change
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "TABLEA");

        // Verify insert
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.schema().fields()).hasSize(2);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("DATA")).isEqualTo(BigDecimal.valueOf(234567.891));

        // Verify DATA column schema definition
        dataSchema = after.schema().field("DATA").schema();
        assertThat(dataSchema.parameters().get("scale")).isEqualTo("3");
        assertThat(dataSchema.parameters().get("connect.decimal.precision")).isEqualTo("10");
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldStreamDropTable() throws Exception {
        createTable("debezium.tablea", "CREATE TABLE debezium.tablea (ID numeric(9,0) primary key, data varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for tablea
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "TABLEA");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert a record into tablea
        connection.execute("INSERT INTO debezium.tablea (id, data) values (1, 'Test')");

        // The record should be emitted based on snapshot schema
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName("DEBEZIUM", "TABLEA"))).hasSize(1);
        record = records.recordsForTopic(topicName("DEBEZIUM", "TABLEA")).get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("DATA")).isEqualTo("Test");

        // Drop the table
        connection.execute("DROP TABLE debezium.tablea");

        // Should emit a single schema change event
        records = consumeRecordsByTopic(1);

        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "TABLEA");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "DROP", "DEBEZIUM", "TABLEA");

        // Should be no more events
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-2916")
    public void shouldSnapshotAndStreamSchemaChangesUsingExplicitCasedNames() throws Exception {
        createTable("debezium.\"tableC\"", "CREATE TABLE debezium.\"tableC\" (\"id\" numeric(9,0) primary key, \"data\" varchar2(50))");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Snapshot should have generated 1 schema change event for "tableC"
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        SourceRecord record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertSnapshotSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        List<Struct> tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("ALTER TABLE debezium.\"tableC\" add \"data2\" number(9,0)");

        // Should generate 1 schema change for tableC
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");
        assertTableChangeColumn(tableChanges.get(0), 2, "data2");

        connection.execute("ALTER TABLE debezium.\"tableC\" add (\"data3\" number(9,0), \"data4\" varchar2(25))");

        // Should generate 1 schema change for tableC
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");
        assertTableChangeColumn(tableChanges.get(0), 2, "data2");
        assertTableChangeColumn(tableChanges.get(0), 3, "data3");
        assertTableChangeColumn(tableChanges.get(0), 4, "data4");

        connection.execute("ALTER TABLE debezium.\"tableC\" drop column \"data3\"");

        // Should generate 1 schema change for tableC
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");
        assertTableChangeColumn(tableChanges.get(0), 2, "data2");
        assertTableChangeColumn(tableChanges.get(0), 3, "data4");

        connection.execute("ALTER TABLE debezium.\"tableC\" rename column \"data4\" to \"Data3\"");

        // Should generate 1 schema change for tableC
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");
        assertTableChangeColumn(tableChanges.get(0), 2, "data2");
        assertTableChangeColumn(tableChanges.get(0), 3, "Data3");

        connection.execute("DROP TABLE debezium.\"tableC\"");

        // Should generate 1 schema change for tableC
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.SERVER_NAME)).hasSize(1);
        record = records.recordsForTopic(TestHelper.SERVER_NAME).get(0);
        assertStreamingSchemaChange(record);
        assertSourceTableInfo(record, "DEBEZIUM", "tableC");

        tableChanges = ((Struct) record.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertTableChange(tableChanges.get(0), "DROP", "DEBEZIUM", "tableC");
        assertTableChangePrimaryKeyNames(tableChanges.get(0), "id");
        assertTableChangeColumn(tableChanges.get(0), 0, "id");
        assertTableChangeColumn(tableChanges.get(0), 1, "data");
        assertTableChangeColumn(tableChanges.get(0), 2, "data2");
        assertTableChangeColumn(tableChanges.get(0), 3, "Data3");
    }

    @Test
    @FixFor("DBZ-2916")
    @Ignore("Test can be flaky and cannot reproduce locally, ignoring to stablize test suite")
    public void shouldNotEmitDdlEventsForNonTableObjects() throws Exception {
        try {
            final LogInterceptor logminerlogInterceptor = new LogInterceptor(AbstractLogMinerEventProcessor.class);
            final LogInterceptor errorLogInterceptor = new LogInterceptor(ErrorHandler.class);
            final LogInterceptor xstreamLogInterceptor = new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");

            // These roles are needed in order to perform certain DDL operations below.
            // Any roles granted here should be revoked in the finally block.
            TestHelper.grantRole("CREATE PROCEDURE");
            TestHelper.grantRole("ALTER ANY PROCEDURE");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // todo: do we need to add more here?
            final int expected = 7;
            connection.execute("CREATE OR REPLACE FUNCTION mytestf() return number is x number(11,2); begin return x; END;");
            connection.execute("DROP FUNCTION mytestf");
            connection.execute("CREATE OR REPLACE PROCEDURE mytest() BEGIN select * from dual; END;");
            connection.execute("DROP PROCEDURE mytest");
            connection.execute("CREATE OR REPLACE PACKAGE pkgtest as function hire return number; END;");
            connection.execute("CREATE OR REPLACE PACKAGE BODY pkgtest as function hire return number; begin return 0; end;");
            connection.execute("DROP PACKAGE pkgtest");

            // Resolve what text to look for depending on connector implementation
            final String logText = ConnectorAdapter.LOG_MINER.equals(TestHelper.adapter()) ? "DDL: " : "Processing DDL event ";

            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> logminerlogInterceptor.countOccurrences(logText) == expected
                            || xstreamLogInterceptor.countOccurrences(logText) == expected);

            stopConnector();
            waitForConnectorShutdown(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Make sure there are no events to process and that no DDL exceptions were logged
            assertThat(errorLogInterceptor.containsMessage("Producer failure")).as("Connector failure").isFalse();
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.revokeRole("ALTER ANY PROCEDURE");
            TestHelper.revokeRole("CREATE PROCEDURE");
        }
    }

    @Test
    @FixFor("DBZ-4037")
    public void shouldParseSchemaChangeWithoutErrorOnFilteredTableWithRawDataType() throws Exception {
        LogInterceptor createTableinterceptor = new LogInterceptor(CreateTableParserListener.class);
        LogInterceptor alterTableinterceptor = new LogInterceptor(AlterTableParserListener.class);
        try {
            TestHelper.dropTable(connection, "dbz4037a");
            TestHelper.dropTable(connection, "dbz4037b");

            connection.execute("CREATE TABLE dbz4037a (id number(9,0), data varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4037a");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4037A")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            // Verify Oracle DDL parser ignores CREATE TABLE with RAW data types
            final String ignoredTable = TestHelper.getDatabaseName() + ".DEBEZIUM.DBZ4037B";
            connection.execute("CREATE TABLE dbz4037b (id number(9,0), data raw(8), primary key(id))");
            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> createTableinterceptor.containsMessage(getIgnoreCreateTable(ignoredTable)));

            // Verify Oracle DDL parser ignores ALTER TABLE with RAW data types
            connection.execute("ALTER TABLE dbz4037b ADD data2 raw(10)");
            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> alterTableinterceptor.containsMessage(getIgnoreAlterTable(ignoredTable)));

            // Capture a simple change on different table
            connection.execute("INSERT INTO dbz4037a (id,data) values (1, 'Test')");
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DEBEZIUM", "DBZ4037A"))).hasSize(1);
            SourceRecord record = records.recordsForTopic(topicName("DEBEZIUM", "DBZ4037A")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test");

            // Check no records to consume
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4037b");
            TestHelper.dropTable(connection, "dbz4037a");
        }
    }

    @Test
    @FixFor("DBZ-4037")
    public void shouldParseSchemaChangeOnTableWithRawDataType() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4037");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4037")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            // Verify that Oracle DDL parser allows RAW column types for CREATE TABLE (included)
            connection.execute("CREATE TABLE dbz4037 (id number(9,0), data raw(8), name varchar(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4037");

            // Verify that Oracle DDL parser allows RAW column types for ALTER TABLE (included)
            connection.execute("ALTER TABLE dbz4037 ADD data2 raw(10)");

            connection.prepareUpdate("INSERT INTO dbz4037 (id,data,name,data2) values (1,?,'Acme 123',?)", preparer -> {
                preparer.setBytes(1, "Test".getBytes());
                preparer.setBytes(2, "T".getBytes());
            });
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic(topicName("DEBEZIUM", "DBZ4037"))).hasSize(1);

            SourceRecord record = records.recordsForTopic(topicName("DEBEZIUM", "DBZ4037")).get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isNull();
            assertThat(after.get("DATA2")).isNull();
            assertThat(after.get("NAME")).isEqualTo("Acme 123");

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4037");
        }
    }

    @Test
    @FixFor("DBZ-4782")
    public void shouldNotResendSchemaChangeIfLastEventReadBeforeRestart() throws Exception {
        TestHelper.dropTable(connection, "dbz4782");
        try {
            connection.execute("CREATE TABLE dbz4782 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz4782");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4782")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("ALTER TABLE dbz4782 add data2 varchar2(50)");

            // CREATE, ALTER
            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> records = sourceRecords.recordsForTopic(TestHelper.SERVER_NAME);
            assertThat(records).hasSize(2);

            assertSnapshotSchemaChange(records.get(0));
            List<Struct> tableChanges = ((Struct) records.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ4782");

            assertStreamingSchemaChange(records.get(1));
            tableChanges = ((Struct) records.get(1).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "DBZ4782");

            // Stop the connector
            stopConnector();

            // Restart connector and verify that we do not re-emit the ALTER table
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Wait for 20 seconds and assert that there are no available records
            waitForAvailableRecords(20, TimeUnit.SECONDS);
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4782");
        }
    }

    @Test
    @FixFor("DBZ-4782")
    public void shouldNotResendSchemaChangeIfLastEventReadBeforeRestartWithFollowupDml() throws Exception {
        TestHelper.dropTable(connection, "dbz4782");
        try {
            createTable("dbz4782", "CREATE TABLE dbz4782 (id numeric(9,0) primary key, data varchar2(50))");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4782")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("ALTER TABLE dbz4782 add data2 varchar2(50)");

            // CREATE, ALTER
            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> records = sourceRecords.recordsForTopic(TestHelper.SERVER_NAME);
            assertThat(records).hasSize(2);

            assertSnapshotSchemaChange(records.get(0));
            List<Struct> tableChanges = ((Struct) records.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ4782");

            assertStreamingSchemaChange(records.get(1));
            tableChanges = ((Struct) records.get(1).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "DBZ4782");

            // Stop the connector
            stopConnector();

            // Restart connector and verify that we do not re-emit the ALTER table
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4782 values (1, 'data1', 'data2')");

            sourceRecords = consumeRecordsByTopic(1);
            records = sourceRecords.recordsForTopic(topicName("DEBEZIUM", "DBZ4782"));
            assertThat(records).hasSize(1);
            VerifyRecord.isValidInsert(records.get(0), "ID", 1);

            // There should be no other records to consume
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4782");
        }
    }

    @Test
    @FixFor("DBZ-4782")
    public void shouldNotResendSchemaChangeWithInprogressTransactionOnSecondTable() throws Exception {
        TestHelper.dropTable(connection, "dbz4782a");
        TestHelper.dropTable(connection, "dbz4782b");
        try {
            createTable("dbz4782a", "CREATE TABLE dbz4782a (id numeric(9,0) primary key, data varchar2(50))");
            createTable("dbz4782b", "CREATE TABLE dbz4782b (id numeric(9,0) primary key, data varchar2(50))");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4782[A|B]")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "regex")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start in-progress transaction for dbz4728b
            try (OracleConnection connection2 = TestHelper.testConnection()) {

                // Perform in-progress operation on second connection & alter the other table in primary connection
                connection2.executeWithoutCommitting("INSERT INTO dbz4782b values (2, 'connection2')");
                connection.execute("ALTER TABLE dbz4782a add data2 varchar2(50)");

                // CREATEx2, ALTER (INSERT isn't here yet, its in progress)
                SourceRecords sourceRecords = consumeRecordsByTopic(3);
                List<SourceRecord> records = sourceRecords.recordsForTopic(TestHelper.SERVER_NAME);
                assertThat(records).hasSize(3);

                assertSnapshotSchemaChange(records.get(0));
                List<Struct> tableChanges = ((Struct) records.get(0).value()).getArray("tableChanges");
                assertThat(tableChanges).hasSize(1);
                assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ4782A");

                assertSnapshotSchemaChange(records.get(1));
                tableChanges = ((Struct) records.get(1).value()).getArray("tableChanges");
                assertThat(tableChanges).hasSize(1);
                assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ4782B");

                assertStreamingSchemaChange(records.get(2));
                tableChanges = ((Struct) records.get(2).value()).getArray("tableChanges");
                assertThat(tableChanges).hasSize(1);
                assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "DBZ4782A");

                // Stop the connector
                stopConnector();

                // Now commit the in-progress transaction while connector is down
                connection2.commit();

                // Restart the connector and verify we don't re-emit the ALTER table; however that we do
                // capture the in-progress transaction correctly when it is committed.
                start(OracleConnector.class, config);
                assertConnectorIsRunning();
                waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

                connection.execute("INSERT INTO dbz4782a values (1, 'data1', 'data2')");

                sourceRecords = consumeRecordsByTopic(2);
                sourceRecords.allRecordsInOrder().forEach(System.out::println);
                records = sourceRecords.recordsForTopic(topicName("DEBEZIUM", "DBZ4782A"));
                assertThat(records).hasSize(1);
                VerifyRecord.isValidInsert(records.get(0), "ID", 1);

                records = sourceRecords.recordsForTopic(topicName("DEBEZIUM", "DBZ4782B"));
                assertThat(records).hasSize(1);
                VerifyRecord.isValidInsert(records.get(0), "ID", 2);
            }

            // There should be no other records to consume
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4782a");
            TestHelper.dropTable(connection, "dbz4782b");
        }
    }

    @Test
    @FixFor("DBZ-5285")
    public void shouldOnlyCaptureSchemaChangesForIncludedTables() throws Exception {
        TestHelper.dropTable(connection, "dbz5285a");
        TestHelper.dropTable(connection, "dbz5285b");
        try {
            connection.execute("CREATE TABLE dbz5285a (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("CREATE TABLE dbz5285b (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5285a");
            TestHelper.streamTable(connection, "dbz5285b");

            connection.execute("INSERT INTO dbz5285a (id,data) values (1, 'A')");
            connection.execute("INSERT INTO dbz5285b (id,data) values (2, 'B')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5285A")
                    .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Create and Insert into DBZ5285A
            SourceRecords records = consumeRecordsByTopic(2);

            // Check schema changes from snapshot
            List<SourceRecord> schemaRecords = records.recordsForTopic("server1");
            assertThat(schemaRecords).hasSize(1);
            assertSnapshotSchemaChange(schemaRecords.get(0));
            List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ5285A");

            // Change state changes from snapshot
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5285A");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("A");

            // There should be no records captured for DBZ5285B
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Change table structure and insert more data.
            connection.execute("ALTER TABLE dbz5285a add data2 varchar2(50)");
            connection.execute("ALTER TABLE dbz5285b add data2 varchar2(50)");
            connection.execute("INSERT INTO dbz5285a (id,data,data2) values (3, 'A3', 'D1')");
            connection.execute("INSERT INTO dbz5285b (id,data,data2) values (4, 'B4', 'D2')");

            // Alter and Insert into DBZ5285A
            records = consumeRecordsByTopic(2);

            // Check schema changes from streaming
            schemaRecords = records.recordsForTopic("server1");
            assertThat(schemaRecords).hasSize(1);
            assertStreamingSchemaChange(schemaRecords.get(0));
            tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "DBZ5285A");

            // Change state changes from streaming
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5285A");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 3);
            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("DATA")).isEqualTo("A3");
            assertThat(after.get("DATA2")).isEqualTo("D1");

            // There should be no records captured for DBZ5285B
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5285b");
            TestHelper.dropTable(connection, "dbz5285a");
        }
    }

    @Test
    @FixFor("DBZ-5285")
    public void shouldCaptureSchemaChangesForAllTablesRegardlessOfIncludeList() throws Exception {
        TestHelper.dropTable(connection, "dbz5285a");
        TestHelper.dropTable(connection, "dbz5285b");
        try {
            connection.execute("CREATE TABLE dbz5285a (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("CREATE TABLE dbz5285b (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5285a");
            TestHelper.streamTable(connection, "dbz5285b");

            connection.execute("INSERT INTO dbz5285a (id,data) values (1, 'A')");
            connection.execute("INSERT INTO dbz5285b (id,data) values (2, 'B')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5285A")
                    .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, false)
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Create for both tables and Insert into DBZ5285A
            SourceRecords records = consumeRecordsByTopic(3);

            // Check schema changes from snapshot
            List<SourceRecord> schemaRecords = records.recordsForTopic("server1");
            assertThat(schemaRecords).hasSize(2);
            assertSnapshotSchemaChange(schemaRecords.get(0));
            List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ5285A");

            assertSnapshotSchemaChange(schemaRecords.get(1));
            tableChanges = ((Struct) schemaRecords.get(1).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "CREATE", "DEBEZIUM", "DBZ5285B");

            // Change state changes from snapshot
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5285A");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("A");

            // There should be no data records captured for DBZ5285B
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Change table structure and insert more data.
            connection.execute("ALTER TABLE dbz5285a add data2 varchar2(50)");
            connection.execute("ALTER TABLE dbz5285b add data2 varchar2(50)");
            connection.execute("INSERT INTO dbz5285a (id,data,data2) values (3, 'A3', 'D1')");
            connection.execute("INSERT INTO dbz5285b (id,data,data2) values (4, 'B4', 'D2')");

            // Alter for both tables and Insert into DBZ5285A
            records = consumeRecordsByTopic(3);

            // Check schema changes from streaming
            schemaRecords = records.recordsForTopic("server1");
            schemaRecords.forEach(System.out::println);
            assertThat(schemaRecords).hasSize(2);
            assertStreamingSchemaChange(schemaRecords.get(0));
            tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);
            assertTableChange(tableChanges.get(0), "ALTER", "DEBEZIUM", "DBZ5285A");

            assertStreamingSchemaChange(schemaRecords.get(1));
            tableChanges = ((Struct) schemaRecords.get(1).value()).getArray("tableChanges");
            assertThat(tableChanges).isEmpty();
            assertSourceTableInfo(schemaRecords.get(1), "DEBEZIUM", "DBZ5285B");

            // Change state changes from streaming
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ5285A");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 3);
            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("DATA")).isEqualTo("A3");
            assertThat(after.get("DATA2")).isEqualTo("D1");

            // There should be no insert records captured for DBZ5285B
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5285b");
            TestHelper.dropTable(connection, "dbz5285a");
        }
    }

    private static String getTableIdString(String schemaName, String tableName) {
        return new TableId(TestHelper.getDatabaseName(), schemaName, tableName).toDoubleQuotedString();
    }

    private void createTable(String tableName, String sql) throws SQLException {
        connection.execute(sql);
        TestHelper.streamTable(connection, tableName);
    }

    private static void assertSnapshotSchemaChange(SourceRecord record) {
        assertThat(record.topic()).isEqualTo(TestHelper.SERVER_NAME);
        assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo(TestHelper.getDatabaseName());
        assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        assertThat(((Struct) record.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
    }

    private static void assertStreamingSchemaChange(SourceRecord record) {
        assertThat(record.topic()).isEqualTo(TestHelper.SERVER_NAME);
        assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo(TestHelper.getDatabaseName());
        assertThat(record.sourceOffset().get("snapshot")).isNull();
        assertThat(((Struct) record.value()).getStruct("source").getString("snapshot")).isNull();
    }

    private static void assertTableChange(Struct change, String type, String schema, String table) {
        assertThat(change.get("type")).isEqualTo(type);
        assertThat(change.get("id")).isEqualTo(getTableIdString(schema, table));
    }

    private static void assertTableChangePrimaryKeyNames(Struct change, String... names) {
        assertThat(change.getStruct("table").getArray("primaryKeyColumnNames")).isEqualTo(Arrays.asList(names));
    }

    private static void assertTableChangeColumn(Struct change, int index, String columnName) {
        List<Struct> columns = change.getStruct("table").getArray("columns");
        assertThat(columns.size()).isGreaterThan(index);
        Struct column = columns.get(index);
        assertThat(column.get("name")).isEqualTo(columnName);
    }

    private static void assertSourceTableInfo(SourceRecord record, String schema, String table) {
        final Struct source = ((Struct) record.value()).getStruct("source");
        assertThat(source.get("db")).isEqualTo(TestHelper.getDatabaseName());
        assertThat(source.get("schema")).isEqualTo(schema);
        assertThat(source.get("table")).isEqualTo(table);
    }

    private static String topicName(String schema, String table) {
        return TestHelper.SERVER_NAME + "." + schema + "." + table;
    }

    private static String getIgnoreCreateTable(String tableName) {
        return "Ignoring CREATE TABLE statement for non-captured table " + tableName;
    }

    private static String getIgnoreAlterTable(String tableName) {
        return "Ignoring ALTER TABLE statement for non-captured table " + tableName;
    }
}
