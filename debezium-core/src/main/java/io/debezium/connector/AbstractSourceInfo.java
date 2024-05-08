/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;

/**
 * Common information provided by all connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfo {

    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String TIMESTAMP_US_KEY = "ts_us";
    public static final String TIMESTAMP_NS_KEY = "ts_ns";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COLLECTION_NAME_KEY = "collection";
    public static final String SEQUENCE_KEY = "sequence";

    private final CommonConnectorConfig config;

    protected AbstractSourceInfo(CommonConnectorConfig config) {
        this.config = config;
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    public Schema schema() {
        return config.getSourceInfoStructMaker().schema();
    }

    protected SourceInfoStructMaker<AbstractSourceInfo> structMaker() {
        return config.getSourceInfoStructMaker();
    }

    /**
     * @return timestamp of the event
     */
    protected abstract Instant timestamp();

    /**
     * @return status whether the record is from snapshot or streaming phase
     */
    protected abstract SnapshotRecord snapshot();

    /**
     * @return name of the database
     */
    protected abstract String database();

    /**
     * @return logical name of the server
     */
    protected String serverName() {
        return config.getLogicalName();
    }

    /**
     * Returns the {@code source} struct representing this source info.
     */
    public Struct struct() {
        return structMaker().struct(this);
    }

    /**
     * Returns extra sequencing metadata about a change event formatted
     * as a stringified JSON array. The metadata contained in a sequence must be
     * ordered sequentially in order to be understood and compared.
     *
     * Note: if a source's sequence metadata contains any string values, those
     * strings must be correctly escaped before being included in the stringified
     * JSON array.
     */
    protected String sequence() {
        return null;
    };

}
