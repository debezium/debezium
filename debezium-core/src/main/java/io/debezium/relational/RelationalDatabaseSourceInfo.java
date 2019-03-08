/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;

/**
 * Common information provided by all relational database connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
public abstract class RelationalDatabaseSourceInfo extends AbstractSourceInfo {
    public static final String DB_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";

    protected static SchemaBuilder schemaBuilder() {
        return AbstractSourceInfo.schemaBuilder()
                .field(DB_NAME_KEY, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .field(SCHEMA_NAME_KEY, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .field(TABLE_NAME_KEY, SchemaBuilder.OPTIONAL_STRING_SCHEMA);
    }

    protected RelationalDatabaseSourceInfo(String version) {
        super(version);
    }

    protected Struct struct(TableId tableId) {
        final Struct sourceInfo = super.struct();

        if (tableId == null) {
            return sourceInfo;
        }
        if (tableId.catalog() != null) {
            sourceInfo.put(DB_NAME_KEY, tableId.catalog());
        }
        if (tableId.schema() != null) {
            sourceInfo.put(SCHEMA_NAME_KEY, tableId.schema());
        }
        if (tableId.table() != null) {
            sourceInfo.put(TABLE_NAME_KEY, tableId.table());
        }
        return sourceInfo;
    }
}
