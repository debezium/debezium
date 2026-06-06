/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.geometry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A semantic type for a Geography class.
 * Used to describe geometries operating on a spherical base (rather than Geometry which operates on a planar basis).
 * This is extremely similar to a Geometry – but they're quite different types and not in a hierarchy.
 * <p>
 * See the Open Geospatial Consortium Simple Features Access specification for details on
 * the Well-Known-Binary format http://www.opengeospatial.org/standards/sfa
 * <p>
 * WKB has to be forwards-compatible to the spec, so strict OGC WKB, and EWKB (PostGIS) are both ok.
 *
 * @author Robert Coup
 */
public class Geography extends Geometry {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Geography";

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
                .field(SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA);
    }
}
