/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi;

import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.util.Collect;

/**
 * An abstract parser of change records. Fields and metadata of change records can be provided by RecordParser.
 */
public abstract class RecordParser {

    private final Struct record;
    private final String id;
    private final String type;
    private final Struct source;
    private final Struct transaction;
    private final String op;
    private final Schema opSchema;
    private final String ts_ms;
    private final Schema ts_msSchema;
    private final Schema dataSchema;
    private final String connectorType;

    protected static final Set<String> SOURCE_FIELDS = Collect.unmodifiableSet(
            AbstractSourceInfo.DEBEZIUM_VERSION_KEY,
            AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY,
            AbstractSourceInfo.SERVER_NAME_KEY,
            AbstractSourceInfo.TIMESTAMP_KEY,
            AbstractSourceInfo.SNAPSHOT_KEY,
            AbstractSourceInfo.DATABASE_NAME_KEY);

    protected RecordParser(RecordAndMetadata recordAndMetadata, String... dataFields) {
        this.record = recordAndMetadata.record();
        this.id = recordAndMetadata.id();
        this.type = recordAndMetadata.type();
        this.source = recordAndMetadata.source();
        this.transaction = recordAndMetadata.transaction();
        this.op = recordAndMetadata.operation();
        this.opSchema = Schema.STRING_SCHEMA;
        this.ts_ms = (String) recordAndMetadata.timestamp().value();
        this.ts_msSchema = recordAndMetadata.timestamp().schema();
        this.connectorType = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        this.dataSchema = recordAndMetadata.dataSchema(dataFields);
    }

    /**
     * Get id of a message
     *.
     * @return id of a message
     */
    public String id() {
        return id;
    }

    /**
     * Get type of a message
     *.
     * @return type of a message
     */
    public String type() {
        return type;
    }

    /**
     * Get the value of the source field in the record.
     *
     * @return the value of the source field
     */
    public Struct source() {
        return source;
    }

    /**
     * Get the value of the transaction field in the record.
     *
     * @return the value of the transaction field
     */
    public Struct transaction() {
        return transaction;
    }

    /**
     * Get the value of the op field in the record.
     *
     * @return the value of the op field
     */
    public String op() {
        return op;
    }

    /**
     * Get the schema of the op field in the record.
     *
     * @return the schema of the op field
     */
    public Schema opSchema() {
        return opSchema;
    }

    /**
     * Get the value of the ts_ms field in the record.
     *
     * @return the value of the ts_ms field
     */
    public String ts_ms() {
        return ts_ms;
    }

    /**
     * Get the schema of the ts_ms field in the record.
     *
     * @return the schema of the ts_ms field
     */
    public Schema ts_msSchema() {
        return ts_msSchema;
    }

    /**
     * Get the schema of the data field in the record; may be not be null.
     */
    public Schema dataSchema() {
        return dataSchema;
    }

    /**
     * Get the value of the data field in the record; may not be null.
     */
    public Struct data() {
        Struct data = new Struct(dataSchema());

        for (Field field : dataSchema.fields()) {
            data.put(field, record.get(field));
        }

        return data;
    }

    /**
     * Get the type of the connector which produced this record
     *.
     * @return the connector type
     */
    public String connectorType() {
        return connectorType;
    }

    /**
     * Search for metadata of the record by name, which are defined in the source field; throw a DataException if not
     * found.
     *
     * @return metadata of the record
     */
    public abstract Object getMetadata(String name);
}
