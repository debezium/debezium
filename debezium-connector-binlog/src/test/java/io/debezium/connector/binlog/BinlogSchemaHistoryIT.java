/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * The test to verify whether DDL is stored correctly in database schema history.
 *
 * @author Jiri Pechanec
 */
public abstract class BinlogSchemaHistoryIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();

    private static final int TABLE_COUNT = 2;

    private UniqueDatabase DATABASE;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE = TestHelper.getUniqueDatabase("history", "history-dbz")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();

        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-3485")
    public void shouldUseQuotedNameInDrop() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        assertDdls(records);
        stopConnector();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreSingleRename() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        assertDdls(records);
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("RENAME TABLE `t-1` TO `new-t-1`");
        }
        records = consumeRecordsByTopic(1);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");

        stopConnector();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreMultipleRenames() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("RENAME TABLE `t-1` TO `new-t-1`, `t.2` TO `new.t.2`");
        }
        records = consumeRecordsByTopic(2);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");
        assertThat(getDdl(schemaChanges, 1)).startsWith("RENAME TABLE `t.2` TO `new.t.2`");

        stopConnector();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreAlterRename() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("ALTER TABLE `t-1` RENAME TO `new-t-1`");
        }
        records = consumeRecordsByTopic(1);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        assertThat(getDdl(schemaChanges, 0)).startsWith("ALTER TABLE `t-1` RENAME TO `new-t-1`");

        stopConnector();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        stopConnector();
    }

    @Test
    public void shouldNotStoreView() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        // Execute a TRUNCATE statement
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
            connection.execute(
                    "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `employee_summary` AS SELECT CONCAT(first_name, ' ', last_name) AS full_name, department_id FROM employees");
            connection.execute(
                    "ALTER ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `employee_summary` AS SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM employees");
            connection.execute("DROP VIEW employee_summary");
        }

        // no DDLs other than create table should be received
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    @Test
    public void shouldNotStoreFunction() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        // Execute a TRUNCATE statement
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE DEFINER=`root`@`localhost` FUNCTION `square`(x INT) RETURNS int\n" +
                    "    DETERMINISTIC\n" +
                    "RETURN x * x");
            connection.execute("DROP FUNCTION square");
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
        }

        // no DDLs other than create table should be received
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    @Test
    public void shouldNotStoreTrigger() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        // Execute a TRUNCATE statement
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
            connection.execute("CREATE DEFINER=`root`@`localhost` TRIGGER trg_before_insert_employees\n" +
                    "BEFORE INSERT ON employees\n" +
                    "FOR EACH ROW\n" +
                    "BEGIN\n" +
                    "    IF NEW.department_id IS NULL THEN\n" +
                    "        SET NEW.department_id = 999;\n" +
                    "    END IF;\n" +
                    "END");
            connection.execute("DROP TRIGGER trg_before_insert_employees");
        }

        // no DDLs other than create table should be received
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    @Test
    public void shouldNotStoreProcedure() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        // Execute a TRUNCATE statement
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
            connection.execute("CREATE DEFINER=`root`@`localhost` PROCEDURE `promote_employee`(IN emp_id INT)\n" +
                    "BEGIN\n" +
                    "    UPDATE employees SET department_id = department_id + 1 WHERE id = emp_id;\n" +
                    "END");
            connection.execute("DROP PROCEDURE promote_employee");
        }

        // no DDLs other than create table should be received
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    @Test
    public void shouldNotStoreTruncateIfSkipped() throws SQLException, InterruptedException {
        skipAvroValidation();
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        // By default, TRUNCATE operations are skipped (debezium.skipped.operations=t)
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("TRUNCATE table `t-1`");
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
        }

        // no DDLs corresponding to TRUNCATE should be received
        records = consumeRecordsByTopic(1);
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    @Test
    public void shouldStoreTruncateIfNotSkipped() throws SQLException, InterruptedException {
        skipAvroValidation();
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.SKIPPED_OPERATIONS, "none")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("TRUNCATE table `t-1`");
            connection.execute("CREATE TABLE employees (\n" +
                    "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
                    "    first_name VARCHAR(50),\n" +
                    "    last_name VARCHAR(50),\n" +
                    "    department_id INT,\n" +
                    "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                    ");");
        }

        // Now we should receive a TRUNCATE record
        records = consumeRecordsByTopic(2);
        final List<SourceRecord> truncateRecords = records.recordsForTopic(DATABASE.topicForTable("t-1"));
        assertThat(truncateRecords).hasSize(1);
        final SourceRecord truncateRecord = truncateRecords.get(0);
        final Struct value = (Struct) truncateRecord.value();
        assertThat(value.getString("op")).isEqualTo("t");

        // but no DDLs should be received
        List<SourceRecord> ddlRecords = records.recordsForTopic(DATABASE.getServerName());
        assertThat(ddlRecords).hasSize(1);
        assertThat(((Struct) ddlRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE employees");
    }

    private void assertDdls(SourceRecords records) {
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        int index = 0;
        assertThat(getDdl(schemaChanges, index++)).startsWith("SET");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t-1`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t.2`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP DATABASE IF EXISTS `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE DATABASE `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("USE `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t-1`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t.2`");
    }

    private String getDdl(final List<SourceRecord> schemaChanges, int index) {
        return ((Struct) schemaChanges.get(index).value()).getString("ddl");
    }
}
