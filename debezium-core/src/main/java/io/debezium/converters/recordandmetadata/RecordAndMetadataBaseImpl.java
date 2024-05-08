/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

public class RecordAndMetadataBaseImpl implements RecordAndMetadata {

    protected static final Set<String> SOURCE_FIELDS = Collect.unmodifiableSet(
            AbstractSourceInfo.DEBEZIUM_VERSION_KEY,
            AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY,
            AbstractSourceInfo.SERVER_NAME_KEY,
            AbstractSourceInfo.TIMESTAMP_KEY,
            AbstractSourceInfo.SNAPSHOT_KEY,
            AbstractSourceInfo.DATABASE_NAME_KEY);

    private final Struct record;
    private final Schema originalDataSchema;

    public RecordAndMetadataBaseImpl(Struct record, Schema originalDataSchema) {
        this.record = record;
        this.originalDataSchema = originalDataSchema;
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

    @Override
    public Struct data(String... dataFields) {
        Schema dataSchema = dataSchema(dataFields);
        Struct data = new Struct(dataSchema);

        for (Field field : dataSchema.fields()) {
            data.put(field, record.get(field));
        }

        return data;
    }

    @Override
    public String connectorType() {
        return source().getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
    }

    @Override
    public Object sourceField(String name, Set<String> connectorSpecificSourceFields) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (connectorSpecificSourceFields.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from " + connectorType() + " connector");
    }
}
