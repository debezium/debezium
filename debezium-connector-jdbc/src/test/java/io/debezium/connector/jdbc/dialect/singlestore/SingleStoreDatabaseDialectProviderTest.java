/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hibernate.community.dialect.SingleStoreDialect;
import org.hibernate.dialect.MySQLDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.singlestore.SingleStoreDatabaseDialect.SingleStoreDatabaseDialectProvider;

/**
 * Unit tests for the SingleStore dialect provider.
 */
@Tag("UnitTests")
class SingleStoreDatabaseDialectProviderTest {

    private final DatabaseDialectProvider provider = new SingleStoreDatabaseDialectProvider();

    @Test
    @DisplayName("Should support Hibernate SingleStore dialect")
    void testSupportsSingleStoreDialect() {
        assertThat(provider.supports(new SingleStoreDialect())).isTrue();
    }

    @Test
    @DisplayName("Should not support a MySQL Hibernate dialect")
    void testDoesNotSupportMySqlDialect() {
        assertThat(provider.supports(new MySQLDialect())).isFalse();
    }

    @Test
    @DisplayName("Should name SingleStore database dialect")
    void testName() {
        assertThat(provider.name()).isEqualTo(SingleStoreDatabaseDialect.class);
    }
}
