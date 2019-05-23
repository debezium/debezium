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

/**
 * Common information provided by all connectors in either source field or offsets.
 * When this class schema changes the connector implementations should create
 * a legacy class that will keep the same behaviour.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfoStructMaker<T extends AbstractSourceInfo> implements SourceInfoStructMaker<T> {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";

    private String version;
    private String connector;
    private String serverName;

    public void init(String connector, String version, String serverName) {
        this.connector = Objects.requireNonNull(connector);
        this.version = Objects.requireNonNull(version);
        this.serverName = Objects.requireNonNull(serverName);
    }

    protected SchemaBuilder commonSchemaBuilder() {
        return SchemaBuilder.struct()
                .field(DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA);
    }

    protected Struct commonStruct() {
        return new Struct(schema())
                .put(DEBEZIUM_VERSION_KEY, version)
                .put(DEBEZIUM_CONNECTOR_KEY, connector)
                .put(SERVER_NAME_KEY, serverName);
    }
}
