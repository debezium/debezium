/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Point;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Point} types.
 * CockroachDB has no PostgreSQL geometric {@code point} type, so points are stored in the native
 * {@code geometry} type from their WKB representation.
 *
 * @author Virag Tripathi
 */
class PointType extends GeometryType {

    public static final PointType INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Point.LOGICAL_NAME };
    }

}
