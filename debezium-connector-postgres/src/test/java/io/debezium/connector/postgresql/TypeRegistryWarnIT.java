/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class TypeRegistryWarnIT implements Testing {

    private static final int INVALID_OID = 0;

    private PostgresConnection connection;
    private LogInterceptor interceptor;
    private Level originalLoggerLevel;

    @BeforeEach
    void before() throws SQLException {
        TestHelper.dropAllSchemas();
        connection = TestHelper.create();
    }

    @AfterEach
    void after() {
        if (interceptor != null) {
            final Logger logger = logger();
            logger.detachAppender(interceptor);
            interceptor.stop();
            // Restores the inherited level when the original level was null
            logger.setLevel(originalLoggerLevel);
            interceptor = null;
        }
        if (connection != null) {
            connection.close();
        }
        try {
            TestHelper.execute(
                    "DROP SCHEMA IF EXISTS dbz1876_a CASCADE;",
                    "DROP SCHEMA IF EXISTS dbz1876_b CASCADE;",
                    "DROP SCHEMA IF EXISTS dbz1876_c CASCADE;",
                    "DROP SCHEMA IF EXISTS dbz1876 CASCADE;");
        }
        catch (Exception e) {
            // Ignore cleanup errors - schemas may have been dropped already
        }
    }

    @Test
    @FixFor("debezium/dbz#1876")
    void shouldNotWarnWhenSameTypeNameExistsInMultipleSchemas() throws SQLException {
        connection.execute(
                "DROP SCHEMA IF EXISTS dbz1876_a CASCADE",
                "DROP SCHEMA IF EXISTS dbz1876_b CASCADE",
                "DROP SCHEMA IF EXISTS dbz1876_c CASCADE",
                "CREATE SCHEMA dbz1876_a",
                "CREATE SCHEMA dbz1876_b",
                "CREATE SCHEMA dbz1876_c",
                "CREATE TABLE dbz1876_a.accounts (id int4 PRIMARY KEY)",
                "CREATE TABLE dbz1876_b.accounts (id int4 PRIMARY KEY)",
                "CREATE TABLE dbz1876_c.accounts (id int4 PRIMARY KEY)",
                "CREATE TYPE dbz1876_a.status AS ENUM ('open')",
                "CREATE TYPE dbz1876_b.status AS ENUM ('open')",
                "CREATE TYPE dbz1876_c.status AS ENUM ('closed')");

        attachInterceptor();
        final TypeRegistry registry = new TypeRegistry(connection);

        assertThat(interceptor.containsWarnMessage("is already mapped"))
                .as("same-named types in different schemas must not be reported as already mapped")
                .isFalse();

        final PostgresType firstAccounts = registry.get("dbz1876_a", "accounts");
        final PostgresType secondAccounts = registry.get("dbz1876_b", "accounts");
        final PostgresType firstStatus = registry.get("dbz1876_a.status");
        final PostgresType secondStatus = registry.get("dbz1876_b.status");

        assertThat(firstAccounts).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(secondAccounts).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(firstAccounts.getOid()).isNotEqualTo(secondAccounts.getOid());
        assertThat(firstStatus).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(secondStatus).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(firstStatus.getOid()).isNotEqualTo(secondStatus.getOid());

        assertThat(interceptor.containsWarnMessage("is already mapped")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1876")
    void shouldWarnOnlyOnceForRepeatedLookupsOfUnknownOid() throws SQLException {
        attachInterceptor(Level.DEBUG);
        final TypeRegistry registry = new TypeRegistry(connection);
        interceptor.clear();

        for (int i = 0; i < 4; i++) {
            assertThat(registry.get(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        }

        assertThat(warnCount(interceptor, "Unknown OID " + INVALID_OID)).isEqualTo(1);
        assertThat(interceptor.getLoggingEvents("Unknown OID " + INVALID_OID))
                .filteredOn(event -> event.getLevel().equals(Level.DEBUG))
                .hasSize(3);
    }

    @Test
    @FixFor("debezium/dbz#1876")
    void shouldWarnOnlyOnceForRepeatedLookupsOfUnknownTypeName() throws SQLException {
        attachInterceptor(Level.DEBUG);
        final TypeRegistry registry = new TypeRegistry(connection);
        interceptor.clear();

        for (int i = 0; i < 3; i++) {
            assertThat(registry.get("dbz1876_missing_type")).isSameAs(PostgresType.UNKNOWN);
        }

        assertThat(warnCount(interceptor, "Unknown type named dbz1876_missing_type")).isEqualTo(1);
        assertThat(interceptor.getLoggingEvents("Unknown type named dbz1876_missing_type"))
                .filteredOn(event -> event.getLevel().equals(Level.DEBUG))
                .hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#1876")
    void shouldWarnOnlyOnceWhenSchemaQualifiedTypeNamesAreRemapped() throws SQLException {
        connection.execute(
                "DROP SCHEMA IF EXISTS dbz1876 CASCADE",
                "CREATE SCHEMA dbz1876",
                "CREATE TYPE dbz1876.remapped_type AS ENUM ('a')",
                "CREATE TYPE dbz1876.other_type AS ENUM ('x')");

        attachInterceptor(Level.DEBUG);
        final TypeRegistry registry = new TypeRegistry(connection);
        interceptor.clear();

        // First re-mapping of the first type emits the single WARN summary for this registry instance
        connection.execute(
                "DROP TYPE dbz1876.remapped_type CASCADE",
                "CREATE TYPE dbz1876.remapped_type AS ENUM ('b')");
        final int remappedSecondOid = typeOid("dbz1876.remapped_type");
        assertThat(registry.get(remappedSecondOid)).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(warnCount(interceptor, "is already mapped")).isEqualTo(1);

        // Re-mapping of another type name must not produce another WARN: the flag is global per registry
        connection.execute(
                "DROP TYPE dbz1876.other_type CASCADE",
                "CREATE TYPE dbz1876.other_type AS ENUM ('y')");
        final int otherSecondOid = typeOid("dbz1876.other_type");
        assertThat(registry.get(otherSecondOid)).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(warnCount(interceptor, "is already mapped")).isEqualTo(1);

        // A further re-mapping of the first type is logged at DEBUG as well
        connection.execute(
                "DROP TYPE dbz1876.remapped_type CASCADE",
                "CREATE TYPE dbz1876.remapped_type AS ENUM ('c')");
        final int remappedThirdOid = typeOid("dbz1876.remapped_type");
        assertThat(registry.get(remappedThirdOid)).isNotEqualTo(PostgresType.UNKNOWN);
        assertThat(warnCount(interceptor, "is already mapped")).isEqualTo(1);

        final List<ILoggingEvent> collisionWarns = interceptor.getLoggingEvents("is already mapped")
                .stream()
                .filter(event -> event.getLevel().equals(Level.WARN))
                .toList();
        assertThat(collisionWarns).hasSize(1);
        assertThat(collisionWarns.get(0).getFormattedMessage()).contains("dbz1876.remapped_type");

        assertThat(interceptor.getLoggingEvents("is already mapped"))
                .filteredOn(event -> event.getLevel().equals(Level.DEBUG))
                .hasSize(3);
    }

    private LogInterceptor attachInterceptor() {
        return attachInterceptor(null);
    }

    private LogInterceptor attachInterceptor(Level level) {
        final Logger logger = logger();
        originalLoggerLevel = logger.getLevel();
        interceptor = new LogInterceptor(TypeRegistry.class);
        if (level != null) {
            logger.setLevel(level);
        }
        return interceptor;
    }

    private static Logger logger() {
        return (Logger) LoggerFactory.getLogger(TypeRegistry.class.getName());
    }

    private int typeOid(String typeName) throws SQLException {
        return connection.queryAndMap("SELECT '" + typeName + "'::regtype::oid", rs -> {
            rs.next();
            return (int) rs.getLong(1);
        });
    }

    private static long warnCount(LogInterceptor interceptor, String text) {
        return interceptor.getLoggingEvents(text).stream()
                .filter(event -> event.getLevel().equals(Level.WARN))
                .count();
    }
}
