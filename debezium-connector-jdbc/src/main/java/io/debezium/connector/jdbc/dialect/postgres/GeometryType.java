/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractGeometryType;
import io.debezium.data.geometry.Geometry;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.spatial.WkbReader;

public class GeometryType extends AbstractGeometryType {

    public static final JdbcType INSTANCE = new GeometryType();

    protected static final String GEO_FROM_WKB_FUNCTION = "%s.ST_GeomFromWKB(?, ?)";
    private static final String TYPE_NAME = "%s.geometry";

    /**
     * The native PostgreSQL geometric types that the connector maps onto the shared {@link Geometry}
     * schema. When a value carries one of these as its propagated source-column type, the sink binds it
     * back to the native type with a plain {@code cast(? as <type>)}, so no PostGIS extension is
     * required; anything else is treated as a genuine PostGIS geometry.
     */
    private static final Set<String> NATIVE_GEOMETRIC_TYPES = Set.of("box", "lseg", "path", "polygon");

    protected String postgisSchema = "public";

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        if (config instanceof JdbcSinkConnectorConfig jdbcConfig) {
            this.postgisSchema = jdbcConfig.getPostgresPostgisSchema();
        }
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        final Optional<String> nativeType = nativeGeometricType(schema);
        if (nativeType.isPresent()) {
            return "cast(? as " + nativeType.get() + ")";
        }
        return String.format(GEO_FROM_WKB_FUNCTION, postgisSchema);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final Optional<String> nativeType = nativeGeometricType(schema);
        if (nativeType.isPresent()) {
            return nativeType.get();
        }
        return String.format(TYPE_NAME, postgisSchema);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        final Optional<String> nativeType = nativeGeometricType(schema);
        if (nativeType.isPresent()) {
            if (value == null) {
                return List.of(new ValueBindDescriptor(index, null));
            }
            return List.of(new ValueBindDescriptor(index, reconstructNativeText(nativeType.get(), requireStruct(value))));
        }
        return super.bind(index, schema, value);
    }

    /**
     * Returns the native PostgreSQL geometric type a value originated from, when the schema carries a
     * propagated source-column type in {@link #NATIVE_GEOMETRIC_TYPES}. The decision is made purely from
     * the schema so that the placeholder count in {@link #getQueryBinding} stays stable across a batch,
     * independent of any individual (possibly null) value.
     */
    private Optional<String> nativeGeometricType(Schema schema) {
        return getSourceColumnType(schema)
                .map(type -> type.toLowerCase(Locale.ROOT))
                .filter(NATIVE_GEOMETRIC_TYPES::contains);
    }

    /**
     * Rebuilds the canonical PostgreSQL text of a native geometric value from the WKB carried on the
     * shared {@link Geometry} struct, so it can be bound to a native column without PostGIS.
     */
    private String reconstructNativeText(String nativeType, Struct struct) {
        final byte[] wkb = struct.getBytes(WKB);
        switch (nativeType) {
            case "box":
                // A box is encoded as a closed 5-point rectangle ring; two opposite corners suffice.
                final List<double[]> boxRing = WkbReader.readPolygonRing(wkb);
                return point(boxRing.get(0)) + "," + point(boxRing.get(2));
            case "lseg":
                return "[" + joinPoints(WkbReader.readLineString(wkb)) + "]";
            case "path":
                final List<double[]> pathPoints = WkbReader.readLineString(wkb);
                return isClosedPath(struct)
                        ? "(" + joinPoints(pathPoints) + ")"
                        : "[" + joinPoints(pathPoints) + "]";
            case "polygon":
                final List<double[]> ring = WkbReader.readPolygonRing(wkb);
                // Drop the duplicated closing vertex that WKB requires but PostgreSQL text does not.
                final List<double[]> vertices = isRingClosed(ring) ? ring.subList(0, ring.size() - 1) : ring;
                return "(" + joinPoints(vertices) + ")";
            default:
                throw unexpectedValue(struct);
        }
    }

    private boolean isClosedPath(Struct struct) {
        if (struct.schema().field(Geometry.EXTENSIONS_FIELD) == null) {
            return false;
        }
        final var extensions = struct.getMap(Geometry.EXTENSIONS_FIELD);
        return extensions != null && Boolean.parseBoolean((String) extensions.get(Geometry.EXTENSION_CLOSED_KEY));
    }

    private static boolean isRingClosed(List<double[]> ring) {
        if (ring.size() < 2) {
            return false;
        }
        final double[] first = ring.get(0);
        final double[] last = ring.get(ring.size() - 1);
        return first[0] == last[0] && first[1] == last[1];
    }

    private static String joinPoints(List<double[]> points) {
        final StringJoiner joiner = new StringJoiner(",");
        for (double[] point : points) {
            joiner.add(point(point));
        }
        return joiner.toString();
    }

    private static String point(double[] point) {
        return "(" + point[0] + "," + point[1] + ")";
    }
}
