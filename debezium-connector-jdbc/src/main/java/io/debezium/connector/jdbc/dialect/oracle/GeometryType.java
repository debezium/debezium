/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.dialect.DatabaseVersion;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractGeoType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Geometry;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class GeometryType extends AbstractGeoType {

    public static final JdbcType INSTANCE = new GeometryType();

    private static final String GEO_FROM_WKB_FUNCTION_ARG1 = "SDO_UTIL.FROM_WKBGEOMETRY(?)";
    private static final String GEO_FROM_WKB_FUNCTION_ARG2 = "SDO_UTIL.FROM_WKBGEOMETRY(?, ?)";
    private static final String TYPE_NAME = "MDSYS.SDO_GEOMETRY";

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return isSridBoundToValue(getDialect().getVersion()) ? GEO_FROM_WKB_FUNCTION_ARG2 : GEO_FROM_WKB_FUNCTION_ARG1;
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return TYPE_NAME;
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (isSridBoundToValue(getDialect().getVersion())) {
            return super.bind(index, schema, value);
        }

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        else if (value instanceof Struct structValue) {
            final byte[] wkb = structValue.getBytes(WKB);
            return List.of(new ValueBindDescriptor(index, wkb));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'",
                getClass().getSimpleName(), value, value.getClass().getName()));
    }

    /**
     * Checks whether the SRID component is bindable. In some versions of Oracle, only the WKB can be
     * bound due to how the function {@code SDO_UTIL.FROM_WKBGEOMETRY} is defined.
     *
     * @param version the database version, should not be {@code null}
     * @return true if the srid value is bound to the database value
     */
    public static boolean isSridBoundToValue(DatabaseVersion version) {
        // Oracle 23 uses 2 arguments (wkb + srid)
        //
        // Oracle 19
        // Jul 2024 - 19.23 definitely uses 1 argument (wkb)
        // Oct 2024 - 19.24 potentially uses 2 arguments
        // Jan 2025 - 19.25 potentially uses 2 arguments
        // Apr 2025 - 19.26 definitely uses 2 arguments (wkb + srid)
        //
        // Oracle 21 and older versions of Oracle use 1 bind argument (wkb)
        return version.isSameOrAfter(23) || (version.isSame(19) && version.isSameOrAfter(19, 26));
    }
}
