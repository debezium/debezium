/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.geometry;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A semantic type for an OGC Simple Features for SQL Geometry.
 * Used to describe geometries on a planar basis (rather than Geography, which is a spherical basis).
 * <p>
 * See the Open Geospatial Consortium Simple Features Access specification for details on
 * the Well-Known-Binary format http://www.opengeospatial.org/standards/sfa
 * <p>
 * WKB has to be forwards-compatible to the spec, so strict OGC WKB, and EWKB (PostGIS) are both ok.
 *
 * @author Robert Coup
 */
public class Geometry {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Geometry";

    public static final String WKB_FIELD = "wkb";
    public static final String SRID_FIELD = "srid";
    public static final String EXTENSIONS_FIELD = "extensions";

    /**
     * Key of the {@link #EXTENSIONS_FIELD} entry that names the native source type a value originated
     * from (for example {@code box}, {@code lseg}, {@code path}, {@code polygon}). Its presence lets a
     * sink distinguish a native geometric type from a genuine PostGIS geometry.
     */
    public static final String EXTENSION_TYPE_KEY = "type";

    /**
     * Key of the {@link #EXTENSIONS_FIELD} entry that records whether a {@code path} is closed
     * ({@code "true"}) or open ({@code "false"}), which WKB itself cannot express.
     */
    public static final String EXTENSION_CLOSED_KEY = "closed";

    /**
     * Returns a {@link SchemaBuilder} for a Geometry field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(2)
                .doc("Geometry")
                .optional()
                .field(WKB_FIELD, Schema.BYTES_SCHEMA)
                .field(SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
                .field(EXTENSIONS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build());
    }

    /**
     * Returns a {@link SchemaBuilder} for a Geometry field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Create a value for this schema using WKB and SRID
     * @param geomSchema a {@link Schema} instance which represents a geometry; may not be null
     * @param wkb OGC Well-Known Binary representation of the geometry; may not be null
     * @param srid the coordinate reference system identifier; may be null if unset/unknown
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema geomSchema, byte[] wkb, Integer srid) {
        Struct result = new Struct(geomSchema);
        result.put(WKB_FIELD, wkb);
        if (srid != null) {
            result.put(SRID_FIELD, srid);
        }
        return result;
    }

    /**
     * Create a value for this schema using WKB, SRID and a set of extensions.
     * <p>
     * Extensions carry information that plain WKB cannot express, for example the native PostgreSQL
     * type a value originated from ({@code type}) or the open/closed nature of a {@code path}. The
     * {@code extensions} field is optional; a {@code null} or empty map leaves it unset, which is how
     * a genuine PostGIS geometry is distinguished from a native geometric type on the sink side.
     *
     * @param geomSchema a {@link Schema} instance which represents a geometry; may not be null
     * @param wkb OGC Well-Known Binary representation of the geometry; may not be null
     * @param srid the coordinate reference system identifier; may be null if unset/unknown
     * @param extensions additional key/value metadata; may be null or empty
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema geomSchema, byte[] wkb, Integer srid, Map<String, String> extensions) {
        Struct result = createValue(geomSchema, wkb, srid);
        if (extensions != null && !extensions.isEmpty()) {
            result.put(EXTENSIONS_FIELD, extensions);
        }
        return result;
    }
}
