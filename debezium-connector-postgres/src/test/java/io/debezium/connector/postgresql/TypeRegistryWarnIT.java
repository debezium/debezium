/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

public class TypeRegistryWarnIT implements Testing {

    @Test
    void shouldNotEmitWarnOnUnknownOidLookup() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            final var logInterceptor = LogInterceptor.forClass(TypeRegistry.class);
            final var registry = new TypeRegistry(connection);
            logInterceptor.clear();

            registry.get(999999);

            assertThat(logInterceptor.containsWarnMessage("Unknown OID")).isFalse();
        }
    }

    @Test
    void shouldNotEmitWarnForRepeatedUnknownOidLookups() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            final var logInterceptor = LogInterceptor.forClass(TypeRegistry.class);
            final var registry = new TypeRegistry(connection);
            logInterceptor.clear();

            registry.get(999999);
            registry.get(888888);
            registry.get(777777);

            assertThat(logInterceptor.countOccurrences("Unknown OID")).isEqualTo(0);
        }
    }

    @Test
    void shouldNotEmitWarnOnUnknownTypeNameLookup() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            final var logInterceptor = LogInterceptor.forClass(TypeRegistry.class);
            final var registry = new TypeRegistry(connection);
            logInterceptor.clear();

            registry.get("nonexistent_type_name");

            assertThat(logInterceptor.containsWarnMessage("Unknown type named")).isFalse();
        }
    }
}
