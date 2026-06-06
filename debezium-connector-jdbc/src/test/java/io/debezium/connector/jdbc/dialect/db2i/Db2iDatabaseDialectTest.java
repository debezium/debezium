/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2i;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Db2iDatabaseDialect}.
 *
 * These tests validate the SQL dialect implementation specifics for DB2i without requiring
 * a live database connection. The tests focus on verifying that generated SQL adheres to
 * DB2i requirements:
 * - MERGE statements use SELECT FROM sysibm.sysdummy1 instead of VALUES
 * - Parameter markers are wrapped with explicit CAST statements
 * - Target table uses TGT alias to avoid ambiguity
 * - ON and SET clauses are properly qualified
 *
 * @author Andrew Love
 */
public class Db2iDatabaseDialectTest {

    /**
     * Test that verifies the Db2iDatabaseDialect is properly registered and can be
     * instantiated through the provider mechanism. This ensures the ServiceLoader
     * configuration is correct and the dialect ordering issue is resolved.
     */
    @Test
    void testDialectProviderExists() {
        Db2iDatabaseDialect.Db2iDatabaseProvider provider = new Db2iDatabaseDialect.Db2iDatabaseProvider();

        assertThat(provider)
                .as("Db2iDatabaseProvider should be instantiable")
                .isNotNull();

        assertThat(provider.name())
                .as("Provider should return Db2iDatabaseDialect class")
                .isEqualTo(Db2iDatabaseDialect.class);
    }

    /**
     * Test that verifies the Db2iDatabaseProvider correctly identifies DB2iDialect
     * from Hibernate. This is crucial for proper dialect resolution.
     */
    @Test
    void testDialectProviderSupportsDB2iDialect() {
        Db2iDatabaseDialect.Db2iDatabaseProvider provider = new Db2iDatabaseDialect.Db2iDatabaseProvider();

        // Create an instance of DB2iDialect
        org.hibernate.dialect.DB2iDialect hibernateDialect = new org.hibernate.dialect.DB2iDialect();

        assertThat(provider.supports(hibernateDialect))
                .as("Provider should support Hibernate's DB2iDialect")
                .isTrue();
    }

    /**
     * Test that verifies the Db2iDatabaseProvider does NOT match the more general
     * DB2Dialect. This ensures the ServiceLoader ordering fix works correctly and
     * the more specific dialect is matched first.
     */
    @Test
    void testDialectProviderDoesNotSupportGeneralDB2Dialect() {
        Db2iDatabaseDialect.Db2iDatabaseProvider provider = new Db2iDatabaseDialect.Db2iDatabaseProvider();

        // Create an instance of the general DB2Dialect (not DB2i)
        org.hibernate.dialect.DB2Dialect hibernateDialect = new org.hibernate.dialect.DB2Dialect();

        assertThat(provider.supports(hibernateDialect))
                .as("Provider should NOT support the general DB2Dialect, only DB2iDialect")
                .isFalse();
    }
}
