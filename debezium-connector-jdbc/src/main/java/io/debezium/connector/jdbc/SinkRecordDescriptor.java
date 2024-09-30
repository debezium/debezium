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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.filter.FieldFilterFactory;
import io.debezium.connector.jdbc.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.SchemaUtils;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;

/**
 * An immutable representation of a {@link SinkRecord}.
 *
 * @author Chris Cranford
 */
@Immutable
public class SinkRecordDescriptor {
    private final SinkRecord record;
    private final String topicName;
    private final List<String> keyFieldNames;
    private final List<String> nonKeyFieldNames;
    private final Map<String, FieldDescriptor> fields;
    private final boolean flattened;

    private SinkRecordDescriptor(SinkRecord record, String topicName, List<String> keyFieldNames, List<String> nonKeyFieldNames,
                                 Map<String, FieldDescriptor> fields, boolean flattened) {
        this.record = record;
        this.topicName = topicName;
        this.keyFieldNames = keyFieldNames;
        this.nonKeyFieldNames = nonKeyFieldNames;
        this.fields = fields;
        this.flattened = flattened;
    }

    public String getTopicName() {
        return topicName;
    }

    public Integer getPartition() {
        return record.kafkaPartition();
    }

    public long getOffset() {
        return record.kafkaOffset();
    }

    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }

    public List<String> getNonKeyFieldNames() {
        return nonKeyFieldNames;
    }

    public Map<String, FieldDescriptor> getFields() {
        return fields;
    }

    public boolean isDebeziumSinkRecord() {
        return !flattened;
    }

    public boolean isTombstone() {
        // NOTE
        // Debezium TOMBSTONE has both value and valueSchema to null, instead the ExtractNewRecordState SMT with delete.handling.mode=none will generate
        // a record only with value null that by JDBC connector is treated as a flattened delete. See isDelete method.
        return record.value() == null && record.valueSchema() == null;
    }

    public boolean isDelete() {
        if (!isDebeziumSinkRecord()) {
            return record.value() == null;
        }
        else if (record.value() != null) {
            final Struct value = (Struct) record.value();
            return Operation.DELETE.equals(Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    public boolean isTruncate() {
        if (isDebeziumSinkRecord()) {
            final Struct value = (Struct) record.value();
            return Operation.TRUNCATE.equals(Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    public Schema getKeySchema() {
        return record.keySchema();
    }

    public Schema getValueSchema() {
        return record.valueSchema();
    }

    public Struct getKeyStruct(PrimaryKeyMode primaryKeyMode) {
        if (!getKeyFieldNames().isEmpty()) {
            switch (primaryKeyMode) {
                case RECORD_KEY:
                    final Schema keySchema = record.keySchema();
                    if (keySchema != null && Schema.Type.STRUCT.equals(keySchema.type())) {
                        return (Struct) record.key();
                    }
                    else {
                        throw new ConnectException("No struct-based primary key defined for record key.");
                    }
                case RECORD_VALUE:
                    final Schema valueSchema = record.valueSchema();
                    if (valueSchema != null && Schema.Type.STRUCT.equals(valueSchema.type())) {
                        return getAfterStruct();
                    }
                    else {
                        throw new ConnectException("No struct-based primary key defined for record value.");
                    }

                case RECORD_HEADER:
                    final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct();
                    record.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));

                    final Schema headerSchema = headerSchemaBuilder.build();
                    final Struct headerStruct = new Struct(headerSchema);
                    record.headers().forEach((Header header) -> headerStruct.put(header.key(), header.value()));
                    return headerStruct;
            }
        }
        return null;
    }

    public Struct getAfterStruct() {
        if (isDebeziumSinkRecord()) {
            return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        }
        else {
            return ((Struct) record.value());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * An immutable representation of a {@link Field} in a {@link SinkRecord}.
     *
     * @author Chris Cranford
     */
    @Immutable
    public static class FieldDescriptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(FieldDescriptor.class);

        private final Schema schema;
        private final String name;
        private final String columnName;
        private final boolean key;
        private final Type type;
        private final DatabaseDialect dialect;
        private final String typeName;

        // Lazily prepared
        private String queryBinding;

        private FieldDescriptor(Schema schema, String name, boolean key, DatabaseDialect dialect) {
            this.schema = schema;
            this.key = key;
            this.dialect = dialect;

            // These are cached here allowing them to be resolved once per record
            this.type = dialect.getSchemaType(schema);
            this.typeName = type.getTypeName(dialect, schema, key);

            this.name = name;
            this.columnName = SchemaUtils.getSourceColumnName(schema).orElse(name);

            LOGGER.trace("Field [{}] with schema [{}]", this.name, schema.type());
            LOGGER.trace("    Type      : {}", type.getClass().getName());
            LOGGER.trace("    Type Name : {}", typeName);
            LOGGER.trace("    Optional  : {}", schema.isOptional());

            if (schema.parameters() != null && !schema.parameters().isEmpty()) {
                LOGGER.trace("    Parameters: {}", schema.parameters());
            }

            if (schema.defaultValue() != null) {
                LOGGER.trace("    Def. Value: {}", schema.defaultValue());
            }
        }

        public Schema getSchema() {
            return schema;
        }

        public String getName() {
            return name;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isKey() {
            return key;
        }

        public Type getType() {
            return type;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getQueryBinding(ColumnDescriptor column, Object value) {
            if (queryBinding == null) {
                queryBinding = type.getQueryBinding(column, schema, value);
            }
            return queryBinding;
        }

        public List<ValueBindDescriptor> bind(int startIndex, Object value) {
            return type.bind(startIndex, schema, value);
        }

        @Override
        public String toString() {
            return "FieldDescriptor{" +
                    "schema=" + schema +
                    ", name='" + name + '\'' +
                    ", key=" + key +
                    ", typeName='" + typeName + '\'' +
                    ", type=" + type +
                    ", columnName='" + columnName + '\'' +
                    '}';
        }
    }

    public static class Builder {

        private static final String CONNECT_TOPIC = "__connect_topic";
        private static final String CONNECT_PARTITION = "__connect_partition";
        private static final String CONNECT_OFFSET = "__connect_offset";

        // External contributed builder state
        private PrimaryKeyMode primaryKeyMode;
        private Set<String> primaryKeyFields;
        private FieldNameFilter fieldFilter = FieldFilterFactory.DEFAULT_FILTER;
        private SinkRecord sinkRecord;
        private DatabaseDialect dialect;

        // Internal build state
        private final List<String> keyFieldNames = new ArrayList<>();
        private final List<String> nonKeyFieldNames = new ArrayList<>();
        private final Map<String, FieldDescriptor> allFields = new LinkedHashMap<>();

        public Builder withDialect(DatabaseDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder withPrimaryKeyFields(Set<String> primaryKeyFields) {
            this.primaryKeyFields = primaryKeyFields;
            return this;
        }

        public Builder withPrimaryKeyMode(PrimaryKeyMode primaryKeyMode) {
            this.primaryKeyMode = primaryKeyMode;
            return this;
        }

        public Builder withSinkRecord(SinkRecord record) {
            this.sinkRecord = record;
            return this;
        }

        public Builder withFieldFilters(FieldNameFilter fieldFilter) {
            this.fieldFilter = fieldFilter;
            return this;
        }

        public SinkRecordDescriptor build() {
            Objects.requireNonNull(primaryKeyMode, "The primary key mode must be provided.");
            Objects.requireNonNull(sinkRecord, "The sink record must be provided.");

            final boolean flattened = !isTombstone(sinkRecord) && isFlattened(sinkRecord);
            final boolean truncated = !flattened && isTruncateEvent(sinkRecord);
            if (!truncated) {
                readSinkRecordKeyData(sinkRecord, flattened);
                readSinkRecordNonKeyData(sinkRecord, flattened);
            }

            return new SinkRecordDescriptor(sinkRecord, sinkRecord.topic(), keyFieldNames, nonKeyFieldNames, allFields, flattened);
        }

        private boolean isFlattened(SinkRecord record) {
            return record.valueSchema().name() == null || !record.valueSchema().name().contains("Envelope");
        }

        private boolean isTombstone(SinkRecord record) {

            return record.value() == null && record.valueSchema() == null;
        }

        private boolean isTruncateEvent(SinkRecord record) {
            return !isTombstone(record)
                    && Operation.TRUNCATE.equals(Operation.forCode(((Struct) record.value()).getString(Envelope.FieldName.OPERATION)));
        }

        private void readSinkRecordKeyData(SinkRecord record, boolean flattened) {
            switch (primaryKeyMode) {
                case NONE:
                    // does nothing
                    break;
                case KAFKA:
                    applyKafkaCoordinatesAsPrimaryKey();
                    break;
                case RECORD_KEY:
                    applyRecordKeyAsPrimaryKey(record);
                    break;
                case RECORD_HEADER:
                    applyRecordHeaderAsPrimaryKey(record);
                    break;
                case RECORD_VALUE:
                    applyRecordValueAsPrimaryKey(record, flattened);
                    break;
                default:
                    throw new ConnectException("Unexpected primary key mode: " + primaryKeyMode);
            }
        }

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

        private void applyRecordKeyAsPrimaryKey(SinkRecord record) {
            final Schema keySchema = record.keySchema();
            if (keySchema == null) {
                throw new ConnectException("Configured primary key mode 'record_key' cannot have null schema");
            }
            else if (keySchema.type().isPrimitive()) {
                applyPrimitiveRecordKeyAsPrimaryKey(keySchema);
            }
            else if (Schema.Type.STRUCT.equals(keySchema.type())) {
                applyRecordKeyAsPrimaryKey(record.topic(), keySchema);
            }
            else {
                throw new ConnectException("An unsupported record key schema type detected: " + keySchema.type());
            }
        }

        private void applyRecordHeaderAsPrimaryKey(SinkRecord record) {
            if (record.headers() == null || record.headers().isEmpty()) {
                throw new ConnectException("Configured primary key mode 'record_header' cannot have null or empty schema");
            }

            final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct();
            record.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));
            final Schema headerSchema = headerSchemaBuilder.build();
            applyRecordKeyAsPrimaryKey(record.topic(), headerSchema);

        }

        private void applyRecordValueAsPrimaryKey(SinkRecord record, boolean flattened) {
            if (primaryKeyFields.isEmpty()) {
                throw new ConnectException("At least one " + JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS +
                        " field name should be specified when resolving keys from the record's value.");
            }

            final Schema valueSchema = record.valueSchema();
            if (valueSchema == null) {
                throw new ConnectException("Configured primary key mode 'record_value' cannot have null schema");
            }
            else if (flattened) {
                for (Field field : record.valueSchema().fields()) {
                    if (primaryKeyFields.contains(field.name())) {
                        addKeyField(record.topic(), field);
                    }
                }
            }
            else {
                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                for (Field field : after.schema().fields()) {
                    if (primaryKeyFields.contains(field.name())) {
                        addKeyField(record.topic(), field);
                    }
                }
            }
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
            if (fieldFilter.matches(topic, field.name())) {
                addKeyField(field.name(), field.schema());
            }
        }

        private void addKeyField(String name, Schema schema) {
            FieldDescriptor fieldDescriptor = new FieldDescriptor(schema, name, true, dialect);
            keyFieldNames.add(fieldDescriptor.getName());
            allFields.put(fieldDescriptor.getName(), fieldDescriptor);
        }

        private void readSinkRecordNonKeyData(SinkRecord record, boolean flattened) {
            final Schema valueSchema = record.valueSchema();
            if (valueSchema != null) {
                if (flattened) {
                    // In a flattened event type, it's safe to read the field names directly
                    // from the schema as this isn't a complex Debezium message type.
                    applyNonKeyFields(record.topic(), valueSchema);
                }
                else {
                    // In a non-flattened event type, this is a complex Debezium type.
                    // We want to source the field names strictly from the 'after' block.
                    final Field after = valueSchema.field(Envelope.FieldName.AFTER);
                    if (after == null) {
                        throw new ConnectException("Received an unexpected message type that does not have an 'after' Debezium block");
                    }
                    applyNonKeyFields(record.topic(), after.schema());
                }
            }
        }

        private void applyNonKeyFields(String topic, Schema schema) {
            for (Field field : schema.fields()) {
                if (!keyFieldNames.contains(field.name())) {
                    if (fieldFilter.matches(topic, field.name())) {
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
}
