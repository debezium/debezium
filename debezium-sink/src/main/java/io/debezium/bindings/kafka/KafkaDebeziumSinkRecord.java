/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.annotation.Immutable;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.util.Strings;

@Immutable
public class KafkaDebeziumSinkRecord implements DebeziumSinkRecord {

    protected final SinkRecord originalKafkaRecord;

    public KafkaDebeziumSinkRecord(SinkRecord record) {
        this.originalKafkaRecord = record;
    }

    @Override
    public String topicName() {
        return originalKafkaRecord.topic();
    }

    @Override
    public Integer partition() {
        return originalKafkaRecord.kafkaPartition();
    }

    @Override
    public long offset() {
        return originalKafkaRecord.kafkaOffset();
    }

    @Override
    public List<String> keyFieldNames() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object key() {
        return originalKafkaRecord.key();
    }

    @Override
    public Schema keySchema() {
        return originalKafkaRecord.keySchema();
    }

    public Object value() {
        return originalKafkaRecord.value();
    }

    @Override
    public Schema valueSchema() {
        return originalKafkaRecord.valueSchema();
    }

    @Override
    public boolean isDebeziumMessage() {
        return originalKafkaRecord.value() != null && originalKafkaRecord.valueSchema().name() != null && originalKafkaRecord.valueSchema().name().contains("Envelope");
    }

    public boolean isSchemaChange() {
        return originalKafkaRecord.valueSchema() != null
                && !Strings.isNullOrEmpty(originalKafkaRecord.valueSchema().name())
                && originalKafkaRecord.valueSchema().name().contains(SCHEMA_CHANGE_VALUE);
    }

    public boolean isFlattened() {
        return !isTombstone() && (originalKafkaRecord.valueSchema().name() == null || !originalKafkaRecord.valueSchema().name().contains("Envelope"));
    }

    @Override
    public boolean isTombstone() {
        // NOTE
        // Debezium TOMBSTONE has both value and valueSchema to null, instead of the ExtractNewRecordState SMT with delete.handling.mode=none
        // which will generate a record with value null that should be treated as a flattened delete. See isDelete method.
        return originalKafkaRecord.value() == null && originalKafkaRecord.valueSchema() == null;
    }

    @Override
    public boolean isDelete() {
        if (!isDebeziumMessage()) {
            return originalKafkaRecord.value() == null;
        }
        else if (originalKafkaRecord.value() != null) {
            final Struct value = (Struct) originalKafkaRecord.value();
            return Envelope.Operation.DELETE.equals(Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    @Override
    public boolean isTruncate() {
        if (isDebeziumMessage()) {
            final Struct value = (Struct) originalKafkaRecord.value();
            return Envelope.Operation.TRUNCATE.equals(Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    public Struct getPayload() {
        if (isDebeziumMessage()) {
            return ((Struct) originalKafkaRecord.value()).getStruct(Envelope.FieldName.AFTER);
        }
        else {
            return ((Struct) originalKafkaRecord.value());
        }
    }

    @Override
    public Struct getKeyStruct(SinkConnectorConfig.PrimaryKeyMode primaryKeyMode, Set<String> primaryKeyFields) {
        if (!keyFieldNames().isEmpty()) {
            switch (primaryKeyMode) {
                case RECORD_KEY -> {
                    final Schema keySchema = originalKafkaRecord.keySchema();
                    if (keySchema != null && Schema.Type.STRUCT.equals(keySchema.type())) {
                        return (Struct) originalKafkaRecord.key();
                    }
                    else {
                        throw new ConnectException("No struct-based primary key defined for record key.");
                    }
                }
                case RECORD_VALUE -> {
                    final Schema valueSchema = originalKafkaRecord.valueSchema();
                    if (valueSchema != null && Schema.Type.STRUCT.equals(valueSchema.type())) {
                        Struct afterPayload = getPayload();
                        if (primaryKeyFields.isEmpty()) {
                            return afterPayload;
                        }
                        else {
                            return buildRecordValueKeyStruct(afterPayload, primaryKeyFields);
                        }
                    }
                    else {
                        throw new ConnectException("No struct-based primary key defined for record value.");
                    }
                }
                case RECORD_HEADER -> {
                    final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct();
                    originalKafkaRecord.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));
                    final Schema headerSchema = headerSchemaBuilder.build();
                    final Struct headerStruct = new Struct(headerSchema);
                    originalKafkaRecord.headers().forEach((Header header) -> headerStruct.put(header.key(), header.value()));
                    return headerStruct;
                }
            }
        }
        return null;
    }

    private Struct buildRecordValueKeyStruct(Struct afterPayload, Set<String> primaryKeyFields) {
        final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        primaryKeyFields.forEach(fieldName -> {
            Field field = afterPayload.schema().field(fieldName);
            keySchemaBuilder.field(field.name(), field.schema());
        });
        Struct keyStruct = new Struct(keySchemaBuilder.build());
        primaryKeyFields.forEach(fieldName -> {
            Field field = afterPayload.schema().field(fieldName);
            keyStruct.put(field.name(), afterPayload.get(field));
        });
        return keyStruct;
    }

    public SinkRecord getOriginalKafkaRecord() {
        return originalKafkaRecord;
    }

}
