/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TypeRegistry} logic that does not require a live database connection.
 *
 * Covers the fixes introduced for dbz#1683: SQL_TYPES ordering and iterative
 * delayed-type resolution to eliminate N+1 SQL_OID_LOOKUP round trips at startup.
 */
public class TypeRegistryTest {

    // --- SQL_TYPES ORDER BY tests ---

    @Test
    void sqlTypesQueryOrdersBaseTypesBeforeDependentTypes() {
        // The ORDER BY ensures base types (typelem=0, typbasetype=0) sort before
        // array types and domains, so prime() can register dependencies in order.
        assertThat(TypeRegistry.SQL_TYPES)
                .contains("ORDER BY (t.typelem != 0 OR t.typbasetype != 0), t.oid");
    }

    @Test
    void sqlTypesQueryExcludesPgToastSchema() {
        assertThat(TypeRegistry.SQL_TYPES)
                .contains("n.nspname != 'pg_toast'");
    }

    // --- PostgresType.Builder getter tests ---

    @Test
    void builderGetElementTypeOidReturnsSetValue() {
        // Verifies that getElementTypeOid() returns the OID set via elementType()
        // This getter is used by canResolveNow() to check dependency readiness
        // without calling builder.build() and triggering SQL_OID_LOOKUP.
        PostgresType.Builder builder = new PostgresType.Builder(
                null, "_int4", 1007, java.sql.Types.ARRAY, 0, null);
        builder.elementType(23); // 23 = int4 OID
        assertThat(builder.getElementTypeOid()).isEqualTo(23);
        assertThat(builder.hasElementType()).isTrue();
    }

    @Test
    void builderGetParentTypeOidReturnsSetValue() {
        // Verifies that getParentTypeOid() returns the OID set via parentType()
        // This getter is used by canResolveNow() to check domain type dependency readiness.
        PostgresType.Builder builder = new PostgresType.Builder(
                null, "my_domain", 99999, java.sql.Types.DISTINCT, 0, null);
        builder.parentType(23); // 23 = int4 OID
        assertThat(builder.getParentTypeOid()).isEqualTo(23);
        assertThat(builder.hasParentType()).isTrue();
    }

    @Test
    void builderHasElementTypeReturnsFalseWhenNotSet() {
        PostgresType.Builder builder = new PostgresType.Builder(
                null, "int4", 23, java.sql.Types.INTEGER, 0, null);
        assertThat(builder.hasElementType()).isFalse();
        assertThat(builder.getElementTypeOid()).isEqualTo(0);
    }

    @Test
    void builderHasParentTypeReturnsFalseWhenNotSet() {
        PostgresType.Builder builder = new PostgresType.Builder(
                null, "int4", 23, java.sql.Types.INTEGER, 0, null);
        assertThat(builder.hasParentType()).isFalse();
        assertThat(builder.getParentTypeOid()).isEqualTo(0);
    }
}
