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
 * A semantic type for a Geography class.
 * Used to describe geometries operating on a spherical base (rather than Geometry which operates on a planar basis).
 * This is extremely similar to a Geometry – but they're quite different types and not in a hierarchy.
 *
 * See the Open Geospatial Consortium Simple Features Access specification for details on
 * the Well-Known-Binary format http://www.opengeospatial.org/standards/sfa
 *
 * WKB has to be forwards-compatible to the spec, so strict OGC WKB, and EWKB (PostGIS) are both ok.
 *
 * @author Robert Coup
 */
public class Geography {
    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Geography";

    public static final String WKB_FIELD = "wkb";
    public static final String SRID_FIELD = "srid";

    /**
     * Returns a {@link SchemaBuilder} for a Geography field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                            .name(LOGICAL_NAME)
                            .version(1)
                            .doc("Geography")
                            .field(WKB_FIELD, Schema.BYTES_SCHEMA)
                            .field(SRID_FIELD, Schema.INT32_SCHEMA);
    }

    /**
     * Returns a {@link SchemaBuilder} for a Geography field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Create a value for this schema using WKB and SRID
     * @param geomSchema a {@link Schema} instance which represents a Geography; may not be null
     * @param wkb OGC Well-Known Binary representation of the geometry; may not be null
     * @param srid the coordinate reference system identifier; may not be null
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema geogSchema, byte[] wkb, int srid){
        Struct result = new Struct(geogSchema);
        result.put(WKB_FIELD, wkb);
        result.put(SRID_FIELD, srid);
        return result;
    }
}
