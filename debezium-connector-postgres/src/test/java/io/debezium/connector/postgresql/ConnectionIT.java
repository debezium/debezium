/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

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

    @Test
    @FixFor("debezium/dbz#2350")
    void shouldReadEveryColumnValueThroughGetColumnValue() throws SQLException {
        try (PostgresConnection conn = TestHelper.createWithTypeRegistry()) {
            conn.execute(
                    "DROP SCHEMA IF EXISTS dbz2350 CASCADE",
                    "CREATE SCHEMA dbz2350",
                    "CREATE TABLE dbz2350.t (id integer, amount numeric(12,2), label text, flag boolean, tags integer[])",
                    "INSERT INTO dbz2350.t VALUES (1, 10.50, 'a', true, '{1,2}')",
                    "INSERT INTO dbz2350.t VALUES (2, 20.75, 'b', false, '{3}')",
                    "INSERT INTO dbz2350.t VALUES (3, 30.00, 'c', true, '{}')");

            Tables tables = new Tables();
            conn.readSchema(tables, null, "dbz2350", null, null, false);
            Table table = tables.forTable(new TableId(null, "dbz2350", "t"));

            List<Object> ids = new ArrayList<>();
            List<Object> labels = new ArrayList<>();
            conn.query("SELECT id, amount, label, flag, tags FROM dbz2350.t ORDER BY id", rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next()) {
                    // Exercise getColumnValue for every column of every row across the array / numeric / default
                    // type paths, ensuring each value decodes correctly (a mis-resolved column type would surface
                    // as a null or wrong value here).
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        Column column = table.columnWithName(metaData.getColumnName(i));
                        Object value = conn.getColumnValue(rs, i, column, table);
                        assertThat(value).as("column %s row-value", column.name()).isNotNull();
                        if ("id".equals(column.name())) {
                            ids.add(value);
                        }
                        else if ("label".equals(column.name())) {
                            labels.add(value);
                        }
                    }
                }
            });

            assertThat(ids).containsExactly(1, 2, 3);
            assertThat(labels).containsExactly("a", "b", "c");
        }
        finally {
            try (PostgresConnection conn = TestHelper.create()) {
                conn.execute("DROP SCHEMA IF EXISTS dbz2350 CASCADE");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2525")
    void shouldRegisterEnumTypeWithNoLabels() throws SQLException {
        try (PostgresConnection conn = TestHelper.create()) {
            conn.connect();
            conn.execute(
                    "DROP SCHEMA IF EXISTS dbz2525 CASCADE",
                    "CREATE SCHEMA dbz2525",
                    "CREATE TYPE dbz2525.empty_enum AS ENUM ()",
                    "CREATE TABLE dbz2525.empty_enum_test (id int4 NOT NULL, value dbz2525.empty_enum, PRIMARY KEY (id))");
        }

        try {
            Configuration config = TestHelper.defaultJdbcConfig();
            JdbcConfiguration jdbcConfig = JdbcConfiguration.adapt(config);

            assertEmptyEnumType(PostgresConnection.createTypeRegistry(jdbcConfig));
            assertEmptyEnumType(PostgresConnection.createTypeRegistry(jdbcConfig, Set.of("dbz2525")));
        }
        finally {
            try (PostgresConnection conn = TestHelper.create()) {
                conn.execute("DROP SCHEMA IF EXISTS dbz2525 CASCADE");
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2041")
    void shouldPrimeDependentTypesWithoutIndividualLookups() throws SQLException {
        try (PostgresConnection conn = TestHelper.create()) {
            conn.execute(
                    "DROP SCHEMA IF EXISTS dbz2041 CASCADE",
                    "CREATE SCHEMA dbz2041",
                    "CREATE DOMAIN dbz2041.base_domain AS varchar(50)",
                    "CREATE DOMAIN dbz2041.dependent_domain AS dbz2041.base_domain");

            // Rewrites the pg_type row of the base domain, so that the types are read back with the
            // dependent one first, as it happens on databases where the catalog has been updated.
            // The rewritten row version takes the lowest free pg_type slot, and catalog churn from
            // earlier tests can leave such slots before the dependent row; each attempt consumes
            // them with filler domains so that the rewrite eventually lands after the dependent one.
            for (int attempt = 0; attempt < 25 && !isDependentDomainReadFirst(conn); attempt++) {
                for (int filler = 0; filler < 32; filler++) {
                    conn.execute(String.format("CREATE DOMAIN dbz2041.filler_%d_%d AS int", attempt, filler));
                }
                conn.execute(String.format("ALTER DOMAIN dbz2041.base_domain %s NOT NULL", attempt % 2 == 0 ? "SET" : "DROP"));
            }

            final long baseTypeOid = conn.queryAndMap(
                    "SELECT 'dbz2041.base_domain'::regtype::oid",
                    rs -> {
                        rs.next();
                        return rs.getLong(1);
                    });
            assertThat(readTypeScanOrder(conn))
                    .as("the types are no longer read with the dependent one first, so debezium/dbz#2041 is not reproduced")
                    .containsSubsequence("dependent_domain", "base_domain");

            final LogInterceptor logInterceptor = new LogInterceptor(TypeRegistry.class);
            logInterceptor.setLoggerLevel(TypeRegistry.class, Level.TRACE);

            TestHelper.getTypeRegistry();

            assertThat(logInterceptor.containsMessage("Priming type registry with database types"))
                    .as("the interceptor is attached and TRACE is enabled")
                    .isTrue();
            // Priming has to register the base domain first, rather than look it up on its own
            assertThat(logInterceptor.containsMessage("Type OID '" + baseTypeOid + "' not cached")).isFalse();
        }
        finally {
            try (PostgresConnection conn = TestHelper.create()) {
                conn.execute("DROP SCHEMA IF EXISTS dbz2041 CASCADE");
            }
        }
    }

    private void assertEmptyEnumType(TypeRegistry typeRegistry) {
        PostgresType emptyEnum = typeRegistry.get("dbz2525", "empty_enum");
        assertThat(emptyEnum).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(emptyEnum.isEnumType()).isTrue();
        assertThat(emptyEnum.getEnumValues()).isEmpty();
    }

    private boolean isDependentDomainReadFirst(PostgresConnection conn) throws SQLException {
        final List<String> scanOrder = readTypeScanOrder(conn);
        return scanOrder.indexOf("dependent_domain") < scanOrder.indexOf("base_domain");
    }

    /**
     * @return the types of the {@code dbz2041} schema, in the order in which the registry reads the types.
     *         The schema is filtered here rather than in the query, as an additional predicate lets
     *         PostgreSQL use an index on pg_type and return the types in a different order.
     */
    private List<String> readTypeScanOrder(PostgresConnection conn) throws SQLException {
        return conn.queryAndMap(TypeRegistry.SQL_TYPES,
                rs -> {
                    final List<String> typeNames = new ArrayList<>();
                    while (rs.next()) {
                        if ("dbz2041".equals(rs.getString("schema_name"))) {
                            typeNames.add(rs.getString("name"));
                        }
                    }
                    return typeNames;
                });
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
