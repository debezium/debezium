/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

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

    // Regression test for schema-qualified type mappings introduced in dbz#1529.
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
    void shouldNotRepeatDatabaseLookupsForUnknownOid() throws SQLException {
        attachInterceptor(Level.TRACE);
        final TypeRegistry registry = new TypeRegistry(connection);
        interceptor.clear();

        // The first lookup of an unknown OID resolves it from the database and logs a warning
        assertThat(registry.get(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        assertThat(oidLookupCount()).isEqualTo(1);
        assertThat(interceptor.containsWarnMessage("Unknown OID " + INVALID_OID + " requested")).isTrue();

        // Subsequent lookups return the cached UNKNOWN without querying the database again
        for (int i = 0; i < 3; i++) {
            assertThat(registry.get(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        }
        assertThat(oidLookupCount()).isEqualTo(1);

        // refresh() evicts the cached entry, so the OID is resolved from the database again
        assertThat(registry.refresh(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        assertThat(oidLookupCount()).isEqualTo(2);

        // After the eviction the next lookup queries the database again and re-caches the result
        assertThat(registry.get(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        assertThat(registry.get(INVALID_OID)).isSameAs(PostgresType.UNKNOWN);
        assertThat(oidLookupCount()).isEqualTo(3);
    }

    private void attachInterceptor() {
        attachInterceptor(null);
    }

    private void attachInterceptor(Level level) {
        final Logger logger = logger();
        originalLoggerLevel = logger.getLevel();
        interceptor = new LogInterceptor(TypeRegistry.class);
        if (level != null) {
            logger.setLevel(level);
        }
    }

    private static Logger logger() {
        return (Logger) LoggerFactory.getLogger(TypeRegistry.class.getName());
    }

    /**
     * Counts how often the registry resolved {@link #INVALID_OID} from the database. Each
     * occurrence of this TRACE message corresponds to one execution of the registry's
     * {@code SQL_OID_LOOKUP} query.
     */
    private long oidLookupCount() {
        return interceptor.getLoggingEvents("Type OID '" + INVALID_OID + "' not cached, attempting to lookup from database").size();
    }
}
