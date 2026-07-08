/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StarRocksDialectResolver}.
 */
@Tag("UnitTests")
class StarRocksDialectResolverTest {

    private final StarRocksDialectResolver resolver = new StarRocksDialectResolver();

    @Test
    @DisplayName("Should resolve the StarRocks dialect for the StarRocks database product name")
    void testResolvesStarRocks() {
        final DialectResolutionInfo info = mock(DialectResolutionInfo.class);
        when(info.getDatabaseName()).thenReturn("StarRocks");

        assertThat(resolver.resolveDialect(info)).isInstanceOf(StarRocksDialect.class);
    }

    @Test
    @DisplayName("Should not resolve a dialect for other database product names")
    void testDoesNotResolveOtherDatabases() {
        final DialectResolutionInfo info = mock(DialectResolutionInfo.class);
        when(info.getDatabaseName()).thenReturn("MySQL");

        assertThat(resolver.resolveDialect(info)).isNull();
    }
}
