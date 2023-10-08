/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

public class RecordAndMetadataBaseImpl implements RecordAndMetadata {

    private final Struct record;

    protected final Schema dataSchema;

    public RecordAndMetadataBaseImpl(Struct record, Schema dataSchema) {
        this.record = record;
        this.dataSchema = dataSchema;
    }

    @Override
    public Struct record() {
        return record;
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        Struct source = record.getStruct(Envelope.FieldName.SOURCE);
        String connectorType = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        return getDataSchema(this.dataSchema, connectorType, dataFields);
    }

    @Override
    public String id() {
        return null;
    }

    @Override
    public String type() {
        return null;
    }

    @Override
    public Struct source() {
        return record.getStruct(Envelope.FieldName.SOURCE);
    }

    @Override
    public String operation() {
        return record.getString(Envelope.FieldName.OPERATION);
    }

    @Override
    public Struct transaction() {
        return record.schema().field(Envelope.FieldName.TRANSACTION) != null ? record.getStruct(Envelope.FieldName.TRANSACTION) : null;
    }

    @Override
    public SchemaAndValue timestamp() {
        String ts_ms = record.getInt64(Envelope.FieldName.TIMESTAMP).toString();
        Schema ts_msSchema = dataSchema.field(Envelope.FieldName.TIMESTAMP).schema();
        return new SchemaAndValue(ts_msSchema, ts_ms);
    }

    private static Schema getDataSchema(Schema schema, String connectorType, String... fields) {
        String dataSchemaName = "io.debezium.connector." + connectorType + ".Data";
        SchemaBuilder builder = SchemaBuilder.struct().name(dataSchemaName);

        for (String field : fields) {
            builder.field(field, schema.field(field).schema());
        }

        return builder.build();
    }
}
