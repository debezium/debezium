/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;

import org.junit.Test;
import org.postgresql.util.PSQLException;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Testing;

public class ConnectionIT implements Testing {

    @Test
    public void shouldDoStuffWithDatabase() throws SQLException {

        try (PostgresConnection conn = TestHelper.create()) {
            conn.connect();
            conn.execute("DROP TABLE IF EXISTS customer");
            conn.execute("create table customer (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")");

            conn.execute("SELECT * FROM customer");
        }
    }

    @Test
    public void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        Configuration config = TestHelper.defaultJdbcConfig().edit()
                .with("query.timeout.ms", "1000").build();

        try (PostgresConnection conn = TestHelper.create(JdbcConfiguration.adapt(config))) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("SELECT pg_sleep(10)"))
                    .isInstanceOf(PSQLException.class)
                    .hasMessage("ERROR: canceling statement due to user request");
        }
    }
}
