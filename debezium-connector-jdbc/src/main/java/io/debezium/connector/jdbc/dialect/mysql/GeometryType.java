/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.Base64;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Geometry;

public class GeometryType extends AbstractType {

    public static final Type INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return "ST_GeomFromWKB(?, ?)";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "geometry";
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {

        if (value == null) {
            query.setParameter(index, null);
            return 1;
        }

        if (value instanceof Struct) {
            final int srid = ((Struct) value).getInt32("srid");
            final byte[] wkb = ((Struct) value).getBytes("wkb");

            // TODO manage binary.handling.mode?
            query.setParameter(index, Base64.getDecoder().decode(wkb));
            query.setParameter(index + 1, srid);
            return 2;
        }

        throwUnexpectedValue(value);
        return 0;
    }
}
