/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.junit.SkipTestDependingOnDatabaseRule;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.MemorySchemaHistory;

/**
 * Tests parsing binlog-based connector column constraints.
 *
 * @author Chris Cranford
 */
@SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "Visible columns not supported")
public abstract class BinlogDdlParserIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-ddl-parser.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "binlog_ddl_parser").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Rule
    public TestRule skipRule = new SkipTestDependingOnDatabaseRule();

    @Before
    public void beforeEach() {
        stopConnector();
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

    protected Configuration.Builder defaultConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName())
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000);
    }

    @Test
    public void parseTableWithVisibleColumns() throws Exception {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SELECT VERSION();");
                connection.execute("CREATE TABLE VISIBLE_COLUMN_TABLE (" +
                        "    ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "    NAME VARCHAR(100) NOT NULL," +
                        "    WORK_ID BIGINT VISIBLE" +
                        ");");
                connection.execute("INSERT INTO VISIBLE_COLUMN_TABLE VALUES (1001,'Larry',113);");
            }
        }

        start(getConnectorClass(), defaultConfig().build());

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DATABASE.getDatabaseName())
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `VISIBLE_COLUMN_TABLE` (\n" +
                                "  `ID` bigint NOT NULL AUTO_INCREMENT,\n" +
                                "  `NAME` varchar(100) NOT NULL,\n" +
                                "  `WORK_ID` bigint DEFAULT NULL,\n" +
                                "  PRIMARY KEY (`ID`)\n" +
                                ") ENGINE=InnoDB AUTO_INCREMENT=1002 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }

    @Test
    public void parseTableWithInVisibleColumns() throws Exception {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SELECT VERSION();");
                connection.execute("CREATE TABLE INVISIBLE_COLUMN_TABLE (" +
                        " ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        " NAME VARCHAR(100) NOT NULL," +
                        " WORK_ID BIGINT INVISIBLE" +
                        ");");
                connection.execute("INSERT INTO INVISIBLE_COLUMN_TABLE VALUES (1002,'Jack');");
            }
        }

        start(getConnectorClass(), defaultConfig().build());

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DATABASE.getDatabaseName())
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `INVISIBLE_COLUMN_TABLE` (\n" +
                                "  `ID` bigint NOT NULL AUTO_INCREMENT,\n" +
                                "  `NAME` varchar(100) NOT NULL,\n" +
                                "  `WORK_ID` bigint DEFAULT NULL /*!80023 INVISIBLE */,\n" +
                                "  PRIMARY KEY (`ID`)\n" +
                                ") ENGINE=InnoDB AUTO_INCREMENT=1003 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }

    @Test
    public void parseTableCreatedWithTableStatement() throws Exception {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("CREATE TABLE table1 (" +
                        "ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "NAME VARCHAR(100) NOT NULL" +
                        ");");
                connection.execute("CREATE TABLE table2 (" +
                        "WORK_ID BIGINT" +
                        ") TABLE table1;");
                connection.execute("INSERT INTO table2 VALUES (113, 1001,'Larry');");
            }
        }

        start(getConnectorClass(), defaultConfig().build());

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DATABASE.getDatabaseName())
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `table2` (\n" +
                                "  `WORK_ID` bigint DEFAULT NULL,\n" +
                                "  `ID` bigint NOT NULL DEFAULT '0',\n" +
                                "  `NAME` varchar(100) NOT NULL\n" +
                                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }
}
