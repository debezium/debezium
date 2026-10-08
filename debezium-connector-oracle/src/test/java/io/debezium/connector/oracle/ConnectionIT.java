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

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
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
    @FixFor("debezium/dbz#2798")
    void shouldResolveTableIdByObjectIdAndDataObjectId() throws SQLException {
        final Configuration config = TestHelper.defaultConfig().build();
        final String catalogName = new OracleConnectorConfig(config).getCatalogName();
        final TableId tableId = new TableId(catalogName, "DEBEZIUM", "DBZ2798");

        try (OracleConnection conn = TestHelper.testConnection(config)) {
            TestHelper.dropTable(conn, "debezium.dbz2798");
            try {
                conn.execute("CREATE TABLE debezium.dbz2798 (id numeric(9,0) primary key)");

                final long objectId = conn.getTableObjectId(tableId);
                final long dataObjectId = conn.getTableDataObjectId(tableId);
                assertThat(conn.getTableIdByObjectId(catalogName, objectId, dataObjectId)).isEqualTo(tableId);

                // Moving the table assigns a new data object id while the object id is retained, so the
                // stale pair must no longer resolve and the current pair must.
                conn.execute("ALTER TABLE debezium.dbz2798 MOVE");
                final long movedDataObjectId = conn.getTableDataObjectId(tableId);
                assertThat(movedDataObjectId).isNotEqualTo(dataObjectId);
                assertThat(conn.getTableIdByObjectId(catalogName, objectId, dataObjectId)).isNull();
                assertThat(conn.getTableIdByObjectId(catalogName, objectId, movedDataObjectId)).isEqualTo(tableId);

                conn.execute("DROP TABLE debezium.dbz2798 PURGE");
                assertThat(conn.getTableIdByObjectId(catalogName, objectId, movedDataObjectId)).isNull();
            }
            finally {
                TestHelper.dropTable(conn, "debezium.dbz2798");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2798")
    void shouldResolveTableIdForSysOwnedTableNotListedInAllObjects() throws SQLException {
        // The connector user cannot select from SYS.OBJ$, so ALL_OBJECTS does not list it. Events for such
        // tables are the ones a physical standby cannot name from a stale dictionary, so the lookup must
        // still resolve them for the filters to skip them.
        try (OracleConnection conn = TestHelper.testConnection()) {
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
