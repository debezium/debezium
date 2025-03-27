/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

public class ConnectionIT implements Testing {

    @Test
    public void shouldDoStuffWithDatabase() throws SQLException {
        int x = 5;
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
    public void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        Configuration config = TestHelper.defaultConfig().with("database.query.timeout.ms", "1000").build();

        try (OracleConnection conn = TestHelper.testConnection(config)) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("begin\n" +
                    "   dbms_lock.sleep(10);\n" +
                    "end;"))
                    .isInstanceOf(SQLTimeoutException.class);
        }
    }
}
