/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.starrocks.StarRocksDatabaseDialect.StarRocksDatabaseDialectProvider;

/**
 * Unit tests for the StarRocks dialect provider.
 */
@Tag("UnitTests")
class StarRocksDatabaseDialectProviderTest {

    private final DatabaseDialectProvider provider = new StarRocksDatabaseDialectProvider();

    @Test
    @DisplayName("Should support Hibernate StarRocks dialect")
    void testSupportsStarRocksDialect() {
        assertThat(provider.supports(new StarRocksDialect())).isTrue();
    }

    @Test
    @DisplayName("Should not support a MySQL Hibernate dialect")
    void testDoesNotSupportMySqlDialect() {
        // StarRocksDialect extends MySQLDialect, so the StarRocks provider must be registered
        // before the MySQL provider; the reverse containment must never match.
        assertThat(provider.supports(new MySQLDialect())).isFalse();
    }

    @Test
    @DisplayName("Should not support a MariaDB Hibernate dialect")
    void testDoesNotSupportMariaDbDialect() {
        assertThat(provider.supports(new MariaDBDialect())).isFalse();
    }

    @Test
    @DisplayName("Should name StarRocks database dialect")
    void testName() {
        assertThat(provider.name()).isEqualTo(StarRocksDatabaseDialect.class);
    }
}
