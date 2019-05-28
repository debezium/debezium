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
 * Legacy source info that does not enforce presence of the version and connector fields
 *
 * @author Jiri Pechanec
 */
public abstract class LegacyV1AbstractSourceInfoStructMaker<T extends AbstractSourceInfo> implements SourceInfoStructMaker<T> {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
 
    private String version;
    private String connector;
    protected String serverName;

    public void init(String connector, String version, String serverName) {
        this.connector = Objects.requireNonNull(connector);
        this.version = Objects.requireNonNull(version);
        this.serverName = serverName;
    }

    protected SchemaBuilder commonSchemaBuilder() {
        return SchemaBuilder.struct()
                .field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DEBEZIUM_CONNECTOR_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected Struct commonStruct() {
        return new Struct(schema())
                .put(DEBEZIUM_VERSION_KEY, version)
                .put(DEBEZIUM_CONNECTOR_KEY, connector);
    }
}
