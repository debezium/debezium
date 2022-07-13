/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.geometry;

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

    /**
     * Returns a {@link SchemaBuilder} for a Geometry field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Geometry")
                .optional()
                .field(WKB_FIELD, Schema.BYTES_SCHEMA)
                .field(SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA);
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
}
