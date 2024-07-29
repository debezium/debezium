/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Geography;

public class GeographyType extends GeometryType {

    public static final Type INSTANCE = new GeographyType();

    private static final String TYPE_NAME = "%s.geography";

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geography.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return String.format(TYPE_NAME, postgisSchema);
    }
}
