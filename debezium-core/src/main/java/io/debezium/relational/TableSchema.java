/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.DataCollectionSchema;

/**
 * Defines the Kafka Connect {@link Schema} functionality associated with a given {@link Table table definition}, and which can
 * be used to send rows of data that match the table definition to Kafka Connect.
 * <p>
 * Given a {@link Table} definition, creating and using a {@link TableSchema} is straightforward:
 *
 * <pre>
 * Table table = ...
 * TableSchema tableSchema = new TableSchemaBuilder().create(table);
 * </pre>
 *
 * or use a subclass of {@link TableSchemaBuilder} for the particular DBMS. Then, for each row of data:
 *
 * <pre>
 * Object[] data = ...
 * Object key = tableSchema.keyFromColumnData(data);
 * Struct value = tableSchema.valueFromColumnData(data);
 * Schema keySchema = tableSchema.keySchema();
 * Schema valueSchema = tableSchema.valueSchema();
 * </pre>
 *
 * all of which can be handed to Kafka Connect to create a new record.
 * <p>
 * When the table structure changes, simply obtain a new or updated {@link Table} definition (e.g., via an {@link Table#edit()
 * editor}), rebuild the {@link TableSchema} for that {@link Table}, and use the new {@link TableSchema} instance for subsequent
 * records.
 *
 * @author Randall Hauch
 * @see TableSchemaBuilder
 */
@Immutable
public class TableSchema implements DataCollectionSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchema.class);

    private final TableId id;
    private final Schema keySchema;
    private final Envelope envelopeSchema;
    private final Schema valueSchema;
    private final StructGenerator keyGenerator;
    private final StructGenerator valueGenerator;

    /**
     * Create an instance with the specified {@link Schema}s for the keys and values, and the functions that generate the
     * key and value for a given row of data.
     *
     * @param id the id of the table corresponding to this schema
     * @param keySchema the schema for the primary key; may be null
     * @param keyGenerator the function that converts a row into a single key object for Kafka Connect; may not be null but may
     *            return nulls
     * @param valueSchema the schema for the values; may be null
     * @param valueGenerator the function that converts a row into a single value object for Kafka Connect; may not be null but
     *            may return nulls
     */
    public TableSchema(TableId id, Schema keySchema, StructGenerator keyGenerator, Envelope envelopeSchema, Schema valueSchema, StructGenerator valueGenerator) {
        this.id = id;
        this.keySchema = keySchema;
        this.envelopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;
        this.keyGenerator = keyGenerator != null ? keyGenerator : (row) -> null;
        this.valueGenerator = valueGenerator != null ? valueGenerator : (row) -> null;
    }

    @Override
    public TableId id() {
        return id;
    }

    /**
     * Get the {@link Schema} that represents the table's columns, excluding those that make up the {@link #keySchema()}.
     *
     * @return the Schema describing the columns in the table; never null
     */
    public Schema valueSchema() {
        return valueSchema;
    }

    /**
     * Get the {@link Schema} that represents the table's primary key.
     *
     * @return the Schema describing the column's that make up the primary key; null if there is no primary key
     */
    @Override
    public Schema keySchema() {
        return keySchema;
    }

    /**
     * Get the {@link Schema} that represents the entire value of messages for the table, i.e. including before/after state
     * and source info.
     *
     * @return the table's envelope schema
     */
    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    /**
     * Convert the specified row of values into a Kafka Connect key. The row is expected to conform to the structured defined
     * by the table.
     *
     * @param columnData the column values for the table
     * @return the key, or null if the {@code columnData}
     */
    public Struct keyFromColumnData(Object[] columnData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("columnData from current stack: {}", maybeRedactSensitiveData(columnData));
        }
        return columnData == null ? null : keyGenerator.generateValue(columnData);
    }

    /**
     * Convert the specified row of values into a Kafka Connect value. The row is expected to conform to the structured defined
     * by the table.
     *
     * @param columnData the column values for the table
     * @return the value, or null if the {@code columnData}
     */
    public Struct valueFromColumnData(Object[] columnData) {
        return columnData == null ? null : valueGenerator.generateValue(columnData);
    }

    @Override
    public int hashCode() {
        return valueSchema().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TableSchema) {
            TableSchema that = (TableSchema) obj;
            return Objects.equals(this.keySchema(), that.keySchema()) && Objects.equals(this.valueSchema(), that.valueSchema());
        }
        return false;
    }

    @Override
    public String toString() {
        return "{ key : " + SchemaUtil.asString(keySchema()) + ", value : " + SchemaUtil.asString(valueSchema()) + " }";
    }
}
