/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
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
    @FixFor("debezium/dbz#2972")
    void shouldRestoreSessionContainerWhenConnectionIsReestablished() throws SQLException {
        assumeTrue(TestHelper.isUsingPdb(), "Requires a pluggable database");

        final String pdbName = new OracleConnectorConfig(TestHelper.defaultConfig().build()).getPdbName();
        try (OracleConnection conn = TestHelper.defaultConnection()) {
            conn.setSessionToPdb(pdbName);
            assertThat(getSessionContainerName(conn)).isEqualToIgnoringCase(pdbName);

            // Simulate the driver losing the physical connection, forcing an in-place reconnect
            conn.connection().close();
            assertThat(getSessionContainerName(conn)).isEqualToIgnoringCase(pdbName);

            conn.resetSessionToCdb();
            assertThat(getSessionContainerName(conn)).isEqualToIgnoringCase("CDB$ROOT");

            conn.connection().close();
            assertThat(getSessionContainerName(conn)).isEqualToIgnoringCase("CDB$ROOT");

            // An explicit reconnect also restores the container
            conn.connection().close();
            conn.reconnect();
            assertThat(getSessionContainerName(conn)).isEqualToIgnoringCase("CDB$ROOT");
        }
    }

    private static String getSessionContainerName(OracleConnection conn) throws SQLException {
        return conn.queryAndMap("SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM DUAL", rs -> {
            assertThat(rs.next()).isTrue();
            return rs.getString(1);
        });
    }
}
