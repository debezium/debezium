/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.MySqlDatabaseVersionResolver;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    protected static final String SERVER_NAME = "is_test";
    public static final int MYSQL8 = 8;
    protected final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "blocking_snapshot_test", "1", null).withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final MySqlDatabaseVersionResolver databaseVersionResolver = new MySqlDatabaseVersionResolver();

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();

            JdbcConnection connection = databaseConnection();
            connection.execute("drop database if exists blocking_snapshot_test_1");

        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 1);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {

        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE_TABLES, DATABASE.qualifiedTableName("a"))
                .with(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, CommonConnectorConfig.SchemaNameAdjustmentMode.AVRO);
    }

    @Override
    protected Configuration.Builder historizedMutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {

        return mutableConfig(signalTableOnly, storeOnlyCapturedDdl)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true);
    }

    @Override
    protected String connector() {
        return "mysql";
    }

    @Override
    protected String server() {
        return DATABASE.getServerName();
    }

    @Override
    protected Class<MySqlConnector> connectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("a");
    }

    @Override
    protected List<String> topicNames() {
        return List.of(DATABASE.topicForTable("a"), DATABASE.topicForTable("b"));
    }

    @Override
    protected String tableName() {
        return tableNameId().toQuotedString('`');
    }

    @Override
    protected List<String> tableNames() {
        final String tableA = TableId.parse(DATABASE.qualifiedTableName("a")).toQuotedString('`');
        final String tableB = TableId.parse(DATABASE.qualifiedTableName("b")).toQuotedString('`');
        return List.of(tableA, tableB);
    }

    @Override
    protected String signalTableName() {
        return tableNameId("debezium_signal").toQuotedString('`');
    }

    @Override
    protected String escapedTableDataCollectionId() {
        return String.format("\\\\\"%s\\\\\".\\\\\"%s\\\\\"", tableNameId().catalog(), tableNameId().table());
    }

    @Override
    protected String signalTableNameSanitized() {
        return DATABASE.qualifiedTableName("debezium_signal");
    }

    @Override
    protected String tableDataCollectionId() {
        return tableNameId().toString();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return List.of(tableNameId().toString(), tableNameId("b").toString());
    }

    private TableId tableNameId() {
        return tableNameId("a");
    }

    private TableId tableNameId(String table) {
        return TableId.parse(DATABASE.qualifiedTableName(table));
    }

    @Override
    protected int expectedDdlsCount() {
        return 12;
    }

    @Override
    protected void assertDdl(List<String> schemaChangesDdls) {

        assertThat(schemaChangesDdls.get(schemaChangesDdls.size() - 2)).isEqualTo("DROP TABLE IF EXISTS `blocking_snapshot_test_1`.`b`");

        assertThat(schemaChangesDdls.get(schemaChangesDdls.size() - 1)).isEqualTo(getDdlString(databaseVersionResolver));

    }

    @NotNull
    private static String getDdlString(MySqlDatabaseVersionResolver databaseVersionResolver) {
        boolean isMariaDB = databaseVersionResolver.isMariaDb();
        if (isMariaDB || databaseVersionResolver.getVersion().getMajor() < MYSQL8) {
            final StringBuilder sb = new StringBuilder("CREATE TABLE `b` (\n");
            sb.append("  `pk` int(11) NOT NULL AUTO_INCREMENT,\n");
            sb.append("  `aa` int(11) DEFAULT NULL,\n");
            sb.append("  PRIMARY KEY (`pk`)\n");
            sb.append(") ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=latin1");
            if (isMariaDB) {
                sb.append(" COLLATE=latin1_swedish_ci");
            }
            return sb.toString();
        }
        else {
            return "CREATE TABLE `b` (\n" +
                    "  `pk` int NOT NULL AUTO_INCREMENT,\n" +
                    "  `aa` int DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`pk`)\n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        }
    }
}
