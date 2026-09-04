/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Testing;

public class ConnectionIT implements Testing {

    @Test
    void shouldDoStuffWithDatabase() throws SQLException {

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
    void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        Configuration config = TestHelper.defaultJdbcConfig().edit()
                .with("query.timeout.ms", "1000").build();

        try (PostgresConnection conn = TestHelper.create(JdbcConfiguration.adapt(config))) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("SELECT pg_sleep(10)"))
                    .isInstanceOf(PSQLException.class)
                    .hasMessage("ERROR: canceling statement due to user request");
        }
    }

    @Test
    @FixFor("debezium/dbz#683")
    void shouldReadUnqualifiedUserDefinedTypeNameRegardlessOfSearchPath() throws SQLException {
        try (PostgresConnection conn = TestHelper.create()) {
            conn.connect();
            conn.execute(
                    "DROP SCHEMA IF EXISTS dbz5571 CASCADE",
                    "CREATE SCHEMA dbz5571",
                    "CREATE TYPE dbz5571.financial_asset AS (quantity numeric, instrument_code text)",
                    "CREATE TYPE dbz5571.mood AS ENUM ('happy', 'sad')",
                    "CREATE DOMAIN dbz5571.positive_int AS integer CHECK (VALUE > 0)",
                    "CREATE TABLE dbz5571.t (id varchar PRIMARY KEY, asset dbz5571.financial_asset, "
                            + "mood dbz5571.mood, amount dbz5571.positive_int, plain integer)");
        }

        // The JDBC driver reports a user-defined type with a schema-qualified name (e.g. "dbz5571"."mood")
        // when the type's schema is not on the search_path, but with the unqualified name otherwise. The
        // streaming path always uses the unqualified name, so the snapshot path must do the same
        // (debezium/dbz#683).
        try {
            assertUnqualifiedTypeNames("public");
            assertUnqualifiedTypeNames("dbz5571, public");
        }
        finally {
            try (PostgresConnection conn = TestHelper.create()) {
                conn.execute("DROP SCHEMA IF EXISTS dbz5571 CASCADE");
            }
        }
    }

    private void assertUnqualifiedTypeNames(String searchPath) throws SQLException {
        try (PostgresConnection conn = TestHelper.createWithTypeRegistry()) {
            conn.execute("SET search_path TO " + searchPath);
            Tables tables = new Tables();
            conn.readSchema(tables, null, "dbz5571", null, null, false);
            Table table = tables.forTable(new TableId(null, "dbz5571", "t"));

            assertThat(table.columnWithName("asset").typeName()).isEqualTo("financial_asset");
            assertThat(table.columnWithName("mood").typeName()).isEqualTo("mood");
            assertThat(table.columnWithName("amount").typeName()).isEqualTo("positive_int");
            // built-in types are unaffected
            assertThat(table.columnWithName("plain").typeName()).isEqualTo("int4");
        }
    }
}
