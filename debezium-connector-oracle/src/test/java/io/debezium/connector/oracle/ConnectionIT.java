/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.relational.OracleColumn;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Testing;

public class ConnectionIT implements Testing {

    @Test
    void shouldDoStuffWithDatabase() throws SQLException {

        Configuration config = TestHelper.testConfig().with("database.query.timeout.ms", "1000").build();

        try (OracleConnection conn = TestHelper.testConnection(config)) {
            conn.connect();
            TestHelper.dropTable(conn, "debezium.customer");
            conn.execute("create table debezium.customer (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar2(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")");

            conn.execute("SELECT * FROM debezium.customer");
        }
    }

    @Test
    void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        Configuration config = TestHelper.defaultConfig().with("database.query.timeout.ms", "1000").build();

        try (OracleConnection conn = TestHelper.testConnection(config)) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("begin\n" +
                    "   dbms_lock.sleep(10);\n" +
                    "end;"))
                    .isInstanceOf(SQLTimeoutException.class);
        }
    }

    @Test
    @FixFor("debezium/dbz#2794")
    void shouldReadColumnOctetLengthFromJdbcMetadataAndDdl() throws SQLException {
        final String ddl = "CREATE TABLE dbz2794 (id numeric(9,0) primary key, c1 varchar2(500 char), "
                + "c2 varchar2(500 byte), c3 varchar2(500), c4 nvarchar2(500), c5 raw(100), c6 number(5))";
        try (OracleConnection connection = TestHelper.testConnection()) {
            TestHelper.dropTable(connection, "dbz2794");
            try {
                connection.execute(ddl);

                final Map<String, Integer> dataLengths = connection.queryAndMap(
                        "SELECT COLUMN_NAME, DATA_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'DBZ2794'",
                        rs -> {
                            final Map<String, Integer> results = new HashMap<>();
                            while (rs.next()) {
                                results.put(rs.getString(1), rs.getInt(2));
                            }
                            return results;
                        });

                final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
                final Tables tables = new Tables(false, connectorConfig);
                connection.readSchema(tables, null, "DEBEZIUM", Tables.TableFilter.fromPredicate(id -> "DBZ2794".equals(id.table())), null, false);
                final Table table = tables.tableIds().stream()
                        .filter(id -> "DBZ2794".equals(id.table()))
                        .findFirst()
                        .map(tables::forTable)
                        .orElse(null);
                assertThat(table).isNotNull();

                for (String columnName : List.of("C1", "C2", "C3", "C4")) {
                    assertThat(table.columnWithName(columnName).unwrap(OracleColumn.class).octetLength())
                            .as(columnName)
                            .isEqualTo(dataLengths.get(columnName));
                }
                // The byte length is only recorded for the string types that can be extended strings
                assertThat(table.columnWithName("C5").unwrap(OracleColumn.class).octetLength()).isEqualTo(Column.UNSET_INT_VALUE);
                assertThat(table.columnWithName("C6").unwrap(OracleColumn.class).octetLength()).isEqualTo(Column.UNSET_INT_VALUE);

                // The DDL parser must resolve the same byte lengths from the database character sets
                final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
                final OracleDdlParser parser = new OracleDdlParser(converters);
                parser.setCurrentSchema("DEBEZIUM");
                final Tables parsedTables = new Tables(false, connectorConfig);
                parser.parse(ddl, parsedTables);
                final Table parsedTable = parsedTables.forTable(new TableId(null, "DEBEZIUM", "DBZ2794"));
                assertThat(parsedTable).isNotNull();
                for (String columnName : List.of("C1", "C2", "C3", "C4")) {
                    assertThat(parsedTable.columnWithName(columnName).unwrap(OracleColumn.class).octetLength())
                            .as(columnName)
                            .isEqualTo(dataLengths.get(columnName));
                }
            }
            finally {
                TestHelper.dropTable(connection, "dbz2794");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    @SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
    void shouldResolveTableIdByObjectIdAndDataObjectId() throws SQLException {
        final Configuration config = TestHelper.defaultConfig().build();
        final String catalogName = new OracleConnectorConfig(config).getCatalogName();
        final TableId tableId = new TableId(catalogName, "DEBEZIUM", "DBZ2798");

        // The table management must be done by the schema user, e.g. testConnection with debezium
        try (OracleConnection testConnection = TestHelper.testConnection()) {
            TestHelper.dropTable(testConnection, "debezium.dbz2798");
            try {
                testConnection.execute("CREATE TABLE debezium.dbz2798 (id numeric(9,0) primary key)");

                // Validation of API calls should be done with connector user, e.g. c##dbzuser
                try (OracleConnection connectorConnection = TestHelper.testConnection(config)) {
                    final long objectId = connectorConnection.getTableObjectId(tableId);
                    final long dataObjectId = connectorConnection.getTableDataObjectId(tableId);
                    assertThat(connectorConnection.getTableIdByObjectId(catalogName, objectId, dataObjectId)).isEqualTo(tableId);

                    // Moving the table assigns a new data object id while the object id is retained, so the
                    // stale pair must no longer resolve and the current pair must.
                    testConnection.execute("ALTER TABLE debezium.dbz2798 MOVE");
                    final long movedDataObjectId = connectorConnection.getTableDataObjectId(tableId);
                    assertThat(movedDataObjectId).isNotEqualTo(dataObjectId);
                    assertThat(connectorConnection.getTableIdByObjectId(catalogName, objectId, dataObjectId)).isNull();
                    assertThat(connectorConnection.getTableIdByObjectId(catalogName, objectId, movedDataObjectId)).isEqualTo(tableId);

                    testConnection.execute("DROP TABLE debezium.dbz2798 PURGE");
                    assertThat(connectorConnection.getTableIdByObjectId(catalogName, objectId, movedDataObjectId)).isNull();
                }
            }
            finally {
                TestHelper.dropTable(testConnection, "debezium.dbz2798");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    @SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
    void shouldResolveTableIdForSysOwnedTableNotListedInAllObjects() throws SQLException {
        // The connector user cannot select from SYS.OBJ$, so ALL_OBJECTS does not list it. Events for such
        // tables are the ones a physical standby cannot name from a stale dictionary, so the lookup must
        // still resolve them for the filters to skip them.
        try (OracleConnection conn = TestHelper.testConnection(TestHelper.defaultConfig().build())) {
            final long[] ids = conn.queryAndMap(
                    "SELECT OBJECT_ID, DATA_OBJECT_ID FROM DBA_OBJECTS WHERE OWNER='SYS' AND OBJECT_NAME='OBJ$' AND OBJECT_TYPE='TABLE'",
                    rs -> {
                        assertThat(rs.next()).isTrue();
                        return new long[]{ rs.getLong(1), rs.getLong(2) };
                    });

            assertThat(conn.getTableIdByObjectId(null, ids[0], ids[1])).isEqualTo(new TableId(null, "SYS", "OBJ$"));
        }
    }
}
