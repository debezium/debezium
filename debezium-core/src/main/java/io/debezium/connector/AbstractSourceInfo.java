/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Common information provided by all connectors in either source field or offsets
 * 
 * @author Jiri Pechanec
 *
 */
public abstract class AbstractSourceInfo {
    public static final String DEBEZIUM_VERSION_KEY = "version";

    private final String version;

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct().field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected AbstractSourceInfo() {
        final String connector = getClass().getName().split("\\.")[3];
        try {
            version = (String)Class.forName("io.debezium.connector." + connector + ".Module").getMethod("version").invoke(null);
        } catch (Exception e) {
            throw new ConnectException("Failed to read module version");
        }
    }

    protected abstract Schema schema();

    protected Struct struct() {
        Struct result = new Struct(schema());
        result.put(DEBEZIUM_VERSION_KEY, version);
        return result;
    }
}
