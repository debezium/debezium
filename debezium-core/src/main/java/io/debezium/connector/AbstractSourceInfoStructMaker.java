/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Enum;
import io.debezium.pipeline.txmetadata.TransactionMonitor;

/**
 * Common information provided by all connectors in either source field or offsets.
 * When this class schema changes the connector implementations should create
 * a legacy class that will keep the same behaviour.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfoStructMaker<T extends AbstractSourceInfo> implements SourceInfoStructMaker<T> {

    public static final Schema SNAPSHOT_RECORD_SCHEMA = Enum.builder("true,last,false").defaultValue("false").optional().build();

    private final String version;
    private final String connector;
    private final String serverName;

    public AbstractSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
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
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_KEY, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);
    }

    protected Struct commonStruct(T sourceInfo) {
        Struct ret = new Struct(schema())
                .put(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, version)
                .put(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, connector)
                .put(AbstractSourceInfo.SERVER_NAME_KEY, serverName)
                .put(AbstractSourceInfo.TIMESTAMP_KEY, sourceInfo.timestamp().toEpochMilli())
                .put(AbstractSourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        final SnapshotRecord snapshot = sourceInfo.snapshot();
        if (snapshot != null) {
            snapshot.toSource(ret);
        }
        return ret;
    }
}
