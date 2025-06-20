/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.common.BaseSourceInfoStructMaker;

/**
 * A factory that creates {@link Struct} instances representing the `source` field
 * in change events emitted by the CockroachDB connector.
 * <p>
 * When CockroachDB changefeed is configured with enriched envelope and enriched_properties=source,updated,
 * this struct may contain:
 * <ul>
 *     <li>db - name of the database (optional)</li>
 *     <li>cluster - name of the CRDB logical cluster (optional)</li>
 *     <li>resolved_ts - resolved timestamp from changefeed</li>
 *     <li>ts_hlc - HLC (Hybrid Logical Clock) timestamp</li>
 *     <li>ts_ns - nanosecond-precision processing timestamp</li>
 * </ul>
 *
 * @author Virag Tripathi
 */
public class CockroachDBSourceInfoStructMaker extends BaseSourceInfoStructMaker<CockroachDBOffsetContext> {

    private static final String FIELD_DB = "db";
    private static final String FIELD_CLUSTER = "cluster";
    private static final String FIELD_RESOLVED = "resolved_ts";
    private static final String FIELD_HLC = "ts_hlc";
    private static final String FIELD_TS_NS = "ts_ns";

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.cockroachdb.Source")
            .field(FIELD_DB, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_CLUSTER, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_RESOLVED, Schema.STRING_SCHEMA)
            .field(FIELD_HLC, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_TS_NS, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private final CockroachDBConnectorConfig config;

    public CockroachDBSourceInfoStructMaker(CockroachDBConnectorConfig config) {
        super("cockroachdb");
        this.config = config;
    }

    @Override
    public Struct sourceInfo(CockroachDBOffsetContext offset) {
        Struct struct = new Struct(SOURCE_SCHEMA);
        struct.put(FIELD_DB, config.databaseName());
        struct.put(FIELD_CLUSTER, config.getLogicalName());
        struct.put(FIELD_RESOLVED, offset.getResolvedTimestamp().toString());

        if (offset.getHlc() != null) {
            struct.put(FIELD_HLC, offset.getHlc());
        }
        if (offset.getTsNs() != null) {
            struct.put(FIELD_TS_NS, offset.getTsNs());
        }

        return struct;
    }

    @Override
    public Schema schema() {
        return SOURCE_SCHEMA;
    }
}
