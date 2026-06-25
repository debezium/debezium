/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;

/**
 * Unit tests for {@link UnsupportedGeospatialType}.
 */
class UnsupportedGeospatialTypeTest {

    @Test
    void testRegistrationKeys() {
        assertThat(Arrays.asList(UnsupportedGeospatialType.INSTANCE.getRegistrationKeys()))
                .containsExactlyInAnyOrder(Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME, Point.LOGICAL_NAME);
    }

    @Test
    void testShouldRejectGeospatialTypeNameResolution() {
        assertThatThrownBy(() -> UnsupportedGeospatialType.INSTANCE.getTypeName(Geometry.schema(), false))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("SingleStore dialect does not support geospatial schema type");
    }

    @Test
    void testShouldRejectGeospatialBinding() {
        assertThatThrownBy(() -> UnsupportedGeospatialType.INSTANCE.bind(1, Point.schema(), null))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("SingleStore dialect does not support geospatial schema type");
    }
}
