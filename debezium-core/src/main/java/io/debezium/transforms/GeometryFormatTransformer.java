/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.data.geometry.Geometry;
import io.debezium.spatial.GeometryBytes;

/**
 * Transformation that converts Geometry formats between WKB(Well Known Binary Format) and EWKB(Extended Well Known Binary Format).
 *
 */
public class GeometryFormatTransformer<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeometryFormatTransformer.class);

    private static final Field SRIDS = Field.create("srids")
            .withDisplayName("Geometry SRIDs that should be considered to include with EWKB")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A comma-separated list of Geometry SRIDs used in converting between WKB and EWKB formats.");

    private SmtManager<R> smtManager;
    private List<Integer> sridList = List.of(4326, 3857, 4269);
    private GeometryFormat geometryFormat = GeometryFormat.WKB;

    /**
     * The set of predefined Geometry format options. This enum implements {@link EnumeratedValue}.
     */
    public enum GeometryFormat implements EnumeratedValue {

        /**
         * Well-Known Binary format
         */
        WKB("wkb"),

        /**
         * Extended Well-Known Binary format
         */
        EWKB("ewkb");

        private final String value;

        GeometryFormat(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    /**
     * @return the configuration definition for this transformation
     */
    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, SRIDS);
        return config;
    }

    /**
     * Configures this transformation with the given key-value pairs.
     * @param configs the configuration key-value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(SRIDS));

        if (config.hasKey(SRIDS)) {
            sridList = config.getList(SRIDS).stream().map(Integer::valueOf).toList();
        }
    }

    @Override
    public void close() {
    }

    /**
     * Applies the transformation to the given record.
     * @param record the record to transform
     * @return the transformed record
     */
    @Override
    public R apply(R record) {
        if (record.value() instanceof Struct structValue) {
            // Modifies the value Struct in-place
            // The schema is not changed, just the WKB byte stream
            processStruct(structValue, structValue.schema());
        }
        return record;
    }

    @Override
    public String version() {
        return Module.version();
    }

    /**
     * Processes a Struct, converting Geometry fields as needed.
     * @param original the original Struct
     * @param schema the Struct schema
     */
    private Struct processStruct(Struct original, Schema schema) {
        schema.fields().forEach(field -> {
            Object value = original.get(field);
            original.put(field, processField(value, field.schema()));
        });
        return original;
    }

    /**
     * Processes an STRUCT schema type and Geometry schema, converting Geometry fields as needed.
     * @param value the field value
     * @param schema the field schema
     * @return the processed field value
     */
    private Object processField(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        if (schema.type() == (Schema.Type.STRUCT)) {
            if (Geometry.LOGICAL_NAME.equals(schema.name())) {
                return processGeometryStruct((Struct) value);
            }
            return processStruct((Struct) value, schema);
        }
        else {
            return value;
        }
    }

    /**
     * Processes a Geometry Struct, converting between WKB and EWKB formats as needed.
     * @param value the Geometry Struct
     * @return the processed Geometry Struct
     */
    private Object processGeometryStruct(Struct value) {
        final Integer srid = value.getInt32(Geometry.SRID_FIELD);
        final byte[] wkb = value.getBytes(GeometryFormat.WKB.getValue());
        final GeometryBytes geometry = new GeometryBytes(wkb, srid);
        // Check the actual format of the geometry wkb. Based on this, convert to the other format.
        if (geometry.isExtended()) {
            geometryFormat = GeometryFormat.EWKB;
        }
        switch (geometryFormat) {
            case WKB -> {
                // Convert to EWKB by adding SRID
                if (srid == null || !sridList.contains(srid)) {
                    String errorMessage = "Cannot convert to EWKB when SRID is null or is not in the configured list: " + srid;
                    LOGGER.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
                return value.put(Geometry.WKB_FIELD, geometry.asExtendedWkb().getBytes());

            }
            case EWKB -> {
                // Convert to WKB by removing SRID
                return value.put(Geometry.WKB_FIELD, geometry.asWkb().getBytes());
            }
            default -> {
                return value;
            }
        }
    }

}
