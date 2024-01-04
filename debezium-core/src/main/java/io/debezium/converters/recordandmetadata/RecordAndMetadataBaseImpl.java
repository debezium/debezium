/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

public class RecordAndMetadataBaseImpl implements RecordAndMetadata {

    private final Struct record;
    private final Schema originalDataSchema;

    public RecordAndMetadataBaseImpl(Struct record, Schema originalDataSchema) {
        this.record = record;
        this.originalDataSchema = originalDataSchema;
    }

    @Override
    public Struct record() {
        return record;
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
        Schema ts_msSchema = originalDataSchema.field(Envelope.FieldName.TIMESTAMP).schema();
        return new SchemaAndValue(ts_msSchema, ts_ms);
    }

    @Override
    public String dataSchemaName() {
        String connectorType = source().getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        return "io.debezium.connector." + connectorType + ".Data";
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        SchemaBuilder builder = SchemaBuilder.struct().name(dataSchemaName());

        if (dataFields.length == 0) {
            // copy fields from original data schema
            for (Field field : originalDataSchema.fields()) {
                builder.field(field.name(), field.schema());
            }
        }
        else {
            for (String field : dataFields) {
                builder.field(field, originalDataSchema.field(field).schema());
            }
        }

        return builder.build();
    }
}
