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

    private final String version;

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct().field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected AbstractSourceInfo(String version) {
        this.version = Objects.requireNonNull(version);
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    protected abstract Schema schema();

    protected Struct struct() {
        Struct result = new Struct(schema());
        result.put(DEBEZIUM_VERSION_KEY, version);
        return result;
    }
}
