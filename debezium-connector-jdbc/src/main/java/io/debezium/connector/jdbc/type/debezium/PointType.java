/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Point;

public class PointType extends GeometryType {

    public static final Type INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Point.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "point";
    }
}
