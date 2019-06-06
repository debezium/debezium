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
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COLLECTION_NAME_KEY = "collection";

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
}
