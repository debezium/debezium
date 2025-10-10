/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Point;
import io.debezium.data.geometry.Geometry;
import org.apache.kafka.connect.data.Struct;
import io.debezium.util.HexConverter;

/**
 * Helper that returns placeholder values for unchanged toasted columns.
 *
 * The configured placeholder is converted to a data type that is compatible with the given column type.
 *
 * @author Praveen Burgu
 */
public class UnchangedToastedPlaceholder {

    private final Map<Object, Object> placeholderValues = new HashMap<Object, Object>();
    private final byte[] toastPlaceholderBinary;
    private final String toastPlaceholderString;
    private final Map<String, String> toastPlaceholderHstore = new HashMap<>();
    private final String toastPlaceholderUuid;
    private final byte[] geometryPlaceholderWkb;
    private final byte[] geographyPlaceholderWkb;

    /**
     * Provides different representations of a placeholder value.<br>
     *
     * <b>NOTE:</b> Adding new types might require an update in {@link io.debezium.processors.reselect.ReselectColumnsPostProcessor}.
     *
     * @param connectorConfig
     */
    public UnchangedToastedPlaceholder(PostgresConnectorConfig connectorConfig) {
        toastPlaceholderBinary = connectorConfig.getUnavailableValuePlaceholder();
        toastPlaceholderString = new String(toastPlaceholderBinary);
        toastPlaceholderUuid = UUID.nameUUIDFromBytes(toastPlaceholderBinary).toString();
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE, toastPlaceholderString);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TEXT_ARRAY_TOAST_VALUE,
                Arrays.asList(toastPlaceholderString));
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_BINARY_ARRAY_TOAST_VALUE,
                Arrays.asList(toastPlaceholderBinary));
        final List<Integer> toastedIntArrayPlaceholder = new ArrayList<>(toastPlaceholderBinary.length);
        final List<Long> toastedLongArrayPlaceholder = new ArrayList<>(toastPlaceholderBinary.length);
        for (byte b : toastPlaceholderBinary) {
            toastedIntArrayPlaceholder.add((int) b);
            toastedLongArrayPlaceholder.add((long) b);
        }
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_INT_ARRAY_TOAST_VALUE, toastedIntArrayPlaceholder);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_BIGINT_ARRAY_TOAST_VALUE, toastedLongArrayPlaceholder);
        toastPlaceholderHstore.put(toastPlaceholderString, toastPlaceholderString);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE, toastPlaceholderHstore);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_UUID_TOAST_VALUE, Arrays.asList(toastPlaceholderUuid));

        // Store WKB data for lazy geometry creation
        // Geometry: 'SRID=4326;POINT(-99999999 -99999999)'::geometry
        geometryPlaceholderWkb = HexConverter.convertFromHex("0101000020E6100000000000FC83D797C1000000FC83D797C1");
        // Geography: 'SRID=4326;POINT(179.999999 89.999999)'::geography
        geographyPlaceholderWkb = HexConverter.convertFromHex("0101000020E61000000C21E7FDFF7F66401842CEFBFF7F5640");

        // These will be created on-demand with the correct field schema
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_POINT_TOAST_VALUE, UnchangedToastedReplicationMessageColumn.UNCHANGED_POINT_TOAST_VALUE);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_GEOMETRY_TOAST_VALUE, UnchangedToastedReplicationMessageColumn.UNCHANGED_GEOMETRY_TOAST_VALUE);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_GEOGRAPHY_TOAST_VALUE, UnchangedToastedReplicationMessageColumn.UNCHANGED_GEOGRAPHY_TOAST_VALUE);

    }

    public Optional<Object> getValue(Object obj) {
        return Optional.ofNullable(placeholderValues.get(obj));
    }

    public byte[] getToastPlaceholderBinary() {
        return toastPlaceholderBinary;
    }

    public String getToastPlaceholderString() {
        return toastPlaceholderString;
    }

    /**
     * Create a geometry placeholder struct using the provided field schema
     */
    public Struct createGeometryPlaceholder(org.apache.kafka.connect.data.Schema fieldSchema) {
        return Geometry.createValue(fieldSchema, geometryPlaceholderWkb, 4326);
    }

    /**
     * Create a geography placeholder struct using the provided field schema
     */
    public Struct createGeographyPlaceholder(org.apache.kafka.connect.data.Schema fieldSchema) {
        return Geography.createValue(fieldSchema, geographyPlaceholderWkb, 4326);
    }

    /**
     * Create a point placeholder struct using the provided field schema
     */
    public Struct createPointPlaceholder(org.apache.kafka.connect.data.Schema fieldSchema) {
        return Point.createValue(fieldSchema, -99999999, -99999999);
    }
}
