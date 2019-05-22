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
 * Common information provided by all connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfo {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";

    private final String version;
    private final String serverName;

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct()
                .field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DEBEZIUM_CONNECTOR_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA);
    }

    protected AbstractSourceInfo(String version, String serverName) {
        this.version = Objects.requireNonNull(version);
        this.serverName = Objects.requireNonNull(serverName);
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    protected abstract Schema schema();

    /**
     * Returns the string that identifies the connector relative to the database. Implementations should override
     * this method to specify the connector identifier.
     *
     * @return the connector identifier
     */
    protected abstract String connector();

    protected Struct struct() {
        return new Struct(schema())
                .put(DEBEZIUM_VERSION_KEY, version)
                .put(DEBEZIUM_CONNECTOR_KEY, connector())
                .put(SERVER_NAME_KEY, serverName);
    }

    public String getServerName() {
        return serverName;
    }
}
