/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.time.Instant;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Enum;

/**
 * Common information provided by all connectors in either source field or offsets.
 * When this class schema changes the connector implementations should create
 * a legacy class that will keep the same behaviour.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfoStructMaker<T extends AbstractSourceInfo> implements SourceInfoStructMaker<T> {

    public static final Schema SNAPSHOT_RECORD_SCHEMA = Enum.builder("true,last,false,incremental").defaultValue("false").optional().build();

    private String version;
    private String connector;
    private String serverName;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        this.connector = Objects.requireNonNull(connector);
        this.version = Objects.requireNonNull(version);
        this.serverName = connectorConfig.getLogicalName();
    }

    protected SchemaBuilder commonSchemaBuilder() {
        return SchemaBuilder.struct()
                .field(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(AbstractSourceInfo.SNAPSHOT_KEY, SNAPSHOT_RECORD_SCHEMA)
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_US_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_NS_KEY, Schema.OPTIONAL_INT64_SCHEMA);
    }

    protected Struct commonStruct(T sourceInfo) {
        final Instant timestamp = sourceInfo.timestamp() == null ? Instant.now() : sourceInfo.timestamp();
        final String database = sourceInfo.database() == null ? "" : sourceInfo.database();
        Struct ret = new Struct(schema())
                .put(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, version)
                .put(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, connector)
                .put(AbstractSourceInfo.SERVER_NAME_KEY, serverName)
                .put(AbstractSourceInfo.TIMESTAMP_KEY, timestamp.toEpochMilli())
                .put(AbstractSourceInfo.DATABASE_NAME_KEY, database)
                .put(AbstractSourceInfo.TIMESTAMP_US_KEY, (timestamp.getEpochSecond() * 1_000_000) + (timestamp.getNano() / 1_000))
                .put(AbstractSourceInfo.TIMESTAMP_NS_KEY, (timestamp.getEpochSecond() * 1_000_000_000L) + timestamp.getNano());
        final String sequence = sourceInfo.sequence();
        if (sequence != null) {
            ret.put(AbstractSourceInfo.SEQUENCE_KEY, sequence);
        }
        final SnapshotRecord snapshot = sourceInfo.snapshot();
        if (snapshot != null) {
            snapshot.toSource(ret);
        }
        return ret;
    }
}
