/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.annotation.Immutable;
import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.data.Envelope;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;

/**
 * An immutable representation of a {@link SinkRecord}.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
@Immutable
public class JdbcKafkaSinkRecord extends KafkaDebeziumSinkRecord implements JdbcSinkRecord {

    private final PrimaryKeyMode primaryKeyMode;
    private final Set<String> primaryKeyFields;
    private final FieldNameFilter fieldsFilter;
    private final DatabaseDialect dialect;
    private final List<String> keyFieldNames = new ArrayList<>();
    private final List<String> nonKeyFieldNames = new ArrayList<>();
    private final Map<String, FieldDescriptor> allFields = new LinkedHashMap<>();

    public JdbcKafkaSinkRecord(SinkRecord record, PrimaryKeyMode primaryKeyMode, Set<String> primaryKeyFields, FieldNameFilter fieldsFilter, DatabaseDialect dialect) {
        super(record);

        Objects.requireNonNull(primaryKeyMode, "The primary key mode must be provided.");
        Objects.requireNonNull(record, "The sink record must be provided.");

        this.primaryKeyMode = primaryKeyMode;
        this.primaryKeyFields = primaryKeyFields;
        this.fieldsFilter = fieldsFilter;
        this.dialect = dialect;

        final var flattened = isFlattened();
        final boolean truncated = !flattened && isTruncate();
        if (!truncated) {
            readSinkRecordKeyData(flattened);
            readSinkRecordNonKeyData(flattened);
        }
    }

    public List<String> keyFieldNames() {
        return keyFieldNames;
    }

    public List<String> getNonKeyFieldNames() {
        return nonKeyFieldNames;
    }

    public Map<String, FieldDescriptor> allFields() {
        return allFields;
    }

    private void readSinkRecordKeyData(boolean flattened) {
        switch (primaryKeyMode) {
            case NONE:
                // does nothing
                break;
            case KAFKA:
                applyKafkaCoordinatesAsPrimaryKey();
                break;
            case RECORD_KEY:
                applyRecordKeyAsPrimaryKey();
                break;
            case RECORD_HEADER:
                applyRecordHeaderAsPrimaryKey();
                break;
            case RECORD_VALUE:
                applyRecordValueAsPrimaryKey(flattened);
                break;
            default:
                throw new ConnectException("Unexpected primary key mode: " + primaryKeyMode);
        }
    }

    private void readSinkRecordNonKeyData(boolean flattened) {
        final Schema valueSchema = valueSchema();
        if (valueSchema != null) {
            if (flattened) {
                // In a flattened event type, it's safe to read the field names directly
                // from the schema as this isn't a complex Debezium message type.
                applyNonKeyFields(topicName(), valueSchema);
            }
            else {
                // In a non-flattened event type, this is a complex Debezium type.
                // We want to source the field names strictly from the 'after' block.
                final Field after = valueSchema.field(Envelope.FieldName.AFTER);
                if (after == null) {
                    throw new ConnectException("Received an unexpected message type that does not have an 'after' Debezium block in topic " + topicName());
                }
                applyNonKeyFields(topicName(), after.schema());
            }
        }
    }

    private static final String CONNECT_TOPIC = "__connect_topic";
    private static final String CONNECT_PARTITION = "__connect_partition";
    private static final String CONNECT_OFFSET = "__connect_offset";

    private void applyKafkaCoordinatesAsPrimaryKey() {
        // CONNECT_TOPIC
        keyFieldNames.add(CONNECT_TOPIC);
        allFields.put(CONNECT_TOPIC, new FieldDescriptor(Schema.STRING_SCHEMA, CONNECT_TOPIC, true, dialect));

        // CONNECT_PARTITION
        keyFieldNames.add(CONNECT_PARTITION);
        allFields.put(CONNECT_PARTITION, new FieldDescriptor(Schema.INT32_SCHEMA, CONNECT_PARTITION, true, dialect));

        // CONNECT_OFFSET
        keyFieldNames.add(CONNECT_OFFSET);
        allFields.put(CONNECT_OFFSET, new FieldDescriptor(Schema.INT64_SCHEMA, CONNECT_OFFSET, true, dialect));
    }

    private void applyRecordKeyAsPrimaryKey() {
        final Schema keySchema = keySchema();
        if (keySchema == null) {
            throw new ConnectException("Configured primary key mode 'record_key' cannot have null schema for topic " + topicName());
        }
        else if (keySchema.type().isPrimitive()) {
            applyPrimitiveRecordKeyAsPrimaryKey(keySchema);
        }
        else if (Schema.Type.STRUCT.equals(keySchema.type())) {
            applyRecordKeyAsPrimaryKey(topicName(), keySchema);
        }
        else {
            throw new ConnectException("An unsupported record key schema type detected: " + keySchema.type() +
                    " for topic " + topicName() + ". The record key schema must be either a primitive or struct type.");
        }
    }

    private void applyRecordHeaderAsPrimaryKey() {
        if (originalKafkaRecord.headers() == null || originalKafkaRecord.headers().isEmpty()) {
            throw new ConnectException("Configured primary key mode 'record_header' cannot have null or empty schema for topic " + topicName());
        }

        final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct();
        originalKafkaRecord.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));
        final Schema headerSchema = headerSchemaBuilder.build();
        applyRecordKeyAsPrimaryKey(topicName(), headerSchema);

    }

    private void applyRecordValueAsPrimaryKey(boolean flattened) {

        final Schema valueSchema = valueSchema();
        if (valueSchema == null) {
            throw new ConnectException("Configured primary key mode 'record_value' cannot have null schema for topic " + topicName());
        }

        Stream<Field> recordFields;
        if (flattened) {
            recordFields = valueSchema().fields().stream();
        }
        else if (isDelete()) {
            recordFields = ((Struct) value()).getStruct(Envelope.FieldName.BEFORE).schema().fields().stream();
        }
        else {
            recordFields = ((Struct) value()).getStruct(Envelope.FieldName.AFTER).schema().fields().stream();
        }

        if (!primaryKeyFields.isEmpty()) {
            recordFields = recordFields.filter(field -> primaryKeyFields.contains(field.name()));
        }

        recordFields.forEach(field -> addKeyField(topicName(), field));
    }

    private void applyPrimitiveRecordKeyAsPrimaryKey(Schema keySchema) {
        if (primaryKeyFields.isEmpty()) {
            throw new ConnectException("The " + JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS +
                    " configuration must be specified when using a primitive key.");
        }
        addKeyField(primaryKeyFields.iterator().next(), keySchema);
    }

    private void applyRecordKeyAsPrimaryKey(String topic, Schema keySchema) {
        for (Field field : keySchema.fields()) {
            if (primaryKeyFields.isEmpty() || primaryKeyFields.contains(field.name())) {
                addKeyField(topic, field);
            }
        }
    }

    private void addKeyField(String topic, Field field) {
        if (fieldsFilter.matches(topic, field.name())) {
            addKeyField(field.name(), field.schema());
        }
    }

    private void addKeyField(String name, Schema schema) {
        FieldDescriptor fieldDescriptor = new FieldDescriptor(schema, name, true, dialect);
        keyFieldNames.add(fieldDescriptor.getName());
        allFields.put(fieldDescriptor.getName(), fieldDescriptor);
    }

    private void applyNonKeyFields(String topic, Schema schema) {
        for (Field field : schema.fields()) {
            if (!keyFieldNames.contains(field.name())) {
                if (fieldsFilter.matches(topic, field.name())) {
                    applyNonKeyField(field.name(), field.schema());
                }
            }
        }
    }

    private void applyNonKeyField(String name, Schema schema) {
        FieldDescriptor fieldDescriptor = new FieldDescriptor(schema, name, false, dialect);
        nonKeyFieldNames.add(fieldDescriptor.getName());
        allFields.put(fieldDescriptor.getName(), fieldDescriptor);
    }

}
