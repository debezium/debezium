/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;

import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The Kafka Connect schemas associated with a TiDB table. Unlike relational connectors that
 * derive table schemas from JDBC metadata or DDL history, the row and key schemas here are taken
 * from the typed TiCDC messages themselves and re-wrapped in a Debezium envelope with the
 * TiDB-specific {@code source} block.
 *
 * @author Aviral Srivastava
 */
public class TiDbTableSchema implements DataCollectionSchema {

    private final TableId id;
    private final Schema keySchema;
    private final Schema rowSchema;
    private final Envelope envelopeSchema;

    public TiDbTableSchema(TableId id, Schema keySchema, Schema rowSchema, Envelope envelopeSchema) {
        this.id = id;
        this.keySchema = keySchema;
        this.rowSchema = rowSchema;
        this.envelopeSchema = envelopeSchema;
    }

    @Override
    public DataCollectionId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    /**
     * @return the schema of a single row of the table, as advertised by TiCDC
     */
    public Schema rowSchema() {
        return rowSchema;
    }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    /**
     * @return {@code true} if this schema was built from the same key and row schemas
     */
    public boolean isCompatibleWith(Schema otherKeySchema, Schema otherRowSchema) {
        return Objects.equals(keySchema, otherKeySchema) && Objects.equals(rowSchema, otherRowSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, keySchema, rowSchema);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TiDbTableSchema) {
            final TiDbTableSchema that = (TiDbTableSchema) obj;
            return Objects.equals(this.id, that.id)
                    && Objects.equals(this.keySchema, that.keySchema)
                    && Objects.equals(this.rowSchema, that.rowSchema);
        }
        return false;
    }

    @Override
    public String toString() {
        return "{ key : " + (keySchema == null ? "null" : SchemaUtil.asString(keySchema))
                + ", value : " + SchemaUtil.asString(envelopeSchema.schema()) + " }";
    }
}
