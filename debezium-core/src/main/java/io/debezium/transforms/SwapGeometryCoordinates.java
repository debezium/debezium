/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.geometry.Geometry;
import io.debezium.spatial.GeometryBytes;

/**
 * A simple transformation that swaps the coordinates within a {@link Geometry} schema type
 * if configured to do so, converting WKB coordinates between the EPSG and GIS coordinate
 * systems.
 *
 * @author Chris Cranford
 */
public class SwapGeometryCoordinates<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Field SRIDS = Field.create("srids")
            .withDisplayName("Geometry SRIDs that should be considered for coordinate swapping")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A comma-separated list of Geometry SRIDs that indicate which geometry fields should be included " +
                    "when swapping coordinates.");

    private SmtManager<R> smtManager;
    private List<Integer> sridList = List.of(4326, 3857, 4269);

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, SRIDS);
        return config;
    }

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

    private Struct processStruct(Struct original, Schema schema) {
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object value = original.get(field);
            original.put(field, processField(value, field.schema()));
        }
        return original;
    }

    private Object processField(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        switch (schema.type()) {
            case STRUCT -> {
                if (Geometry.LOGICAL_NAME.equals(schema.name())) {
                    return processGeometryStruct((Struct) value);
                }
                return processStruct((Struct) value, schema);
            }
            case ARRAY -> {
                return ((List<?>) value).stream()
                        .map(item -> processField(item, schema.valueSchema()))
                        .toList();
            }
            case MAP -> {
                return ((Map<?, ?>) value).entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> processField(e.getKey(), schema.keySchema()),
                                e -> processField(e.getValue(), schema.valueSchema())));
            }
            default -> {
                return value;
            }
        }
    }

    private Object processGeometryStruct(Struct value) {
        // In some cases the SRID is optional.
        final Integer srid = value.getInt32(Geometry.SRID_FIELD);
        if (srid == null) {
            // Without an SRID, bytes cannot be flipped.
            return value;
        }

        final byte[] wkb = value.getBytes(Geometry.WKB_FIELD);
        if (sridList.contains(srid)) {
            final GeometryBytes geometry = new GeometryBytes(wkb, srid);
            value.put(Geometry.WKB_FIELD, geometry.swapCoordinatesNoCheck().getWkb());
        }

        return value;
    }

}
