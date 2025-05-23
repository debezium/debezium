/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.util.Strings;

@Immutable
public class KafkaDebeziumSinkRecord implements DebeziumSinkRecord {

    protected final String cloudEventsSchemaNamePattern;
    protected final SinkRecord originalKafkaRecord;
    private final Struct kafkaCoordinates;
    private final Struct kafkaPayload;
    private final Struct kafkaHeader;
    private Boolean isDebeziumMessage = null;
    private Boolean isFlattened = null;
    private Boolean isDelete = null;
    private Optional<Object> cachedValue = null;
    private final Map<String, FieldDescriptor> kafkaFields = new LinkedHashMap<>();

    protected final Map<String, FieldDescriptor> allFields = new LinkedHashMap<>();

    public KafkaDebeziumSinkRecord(SinkRecord record, String cloudEventsSchemaNamePattern) {
        Objects.requireNonNull(record, "The sink record must be provided.");

        this.cloudEventsSchemaNamePattern = null == cloudEventsSchemaNamePattern ? CloudEventsMaker.CLOUDEVENTS_SCHEMA_SUFFIX : cloudEventsSchemaNamePattern;
        this.originalKafkaRecord = record;
        this.kafkaCoordinates = getKafkaCoordinates();
        this.kafkaHeader = getRecordHeaders();

        if (isDelete()) {
            this.kafkaPayload = getKafkaPayload(!isDebeziumMessage());
        }
        else {
            final var flattened = isFlattened();
            if (flattened || !isTruncate()) {
                this.kafkaPayload = getKafkaPayload(flattened);
            }
            else {
                this.kafkaPayload = null;
            }
        }
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
    public Struct kafkaCoordinates() {
        return kafkaCoordinates;
    }

    @Override
    public Struct kafkaHeader() {
        return kafkaHeader;
    }

    @Override
    public Map<String, FieldDescriptor> allFields() {
        return allFields;
    }

    @Override
    public Object key() {
        return originalKafkaRecord.key();
    }

    @Override
    public Schema keySchema() {
        return originalKafkaRecord.keySchema();
    }

    private static Struct buildStructFromMap(Map<?, ?> map, Schema schema) {
        Struct struct = new Struct(schema);
        schema.fields().forEach(field -> {
            switch (field.schema().type()) {
                case STRUCT -> struct.put(field.name(), buildStructFromMap((Map<?, ?>) map.get(field.name()), field.schema()));
                default -> struct.put(field.name(), map.get(field.name()));
            }
        });
        return struct;
    }

    private static class KafkaValues extends Values {

        public static Schema inferSchemaFromMap(Map<?, ?> map) {
            if (map.isEmpty()) {
                return null;
            }
            boolean isMap = true;
            Values.SchemaDetector keyDetector = new Values.SchemaDetector();
            Values.SchemaDetector valueDetector = new Values.SchemaDetector();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!keyDetector.canDetect(entry.getKey()) || !valueDetector.canDetect(entry.getValue())) {
                    isMap = false; // it is in fact a Struct based on a Map<?, ?> or Map<Object, Object>
                    break;
                }
            }
            if (isMap) {
                return SchemaBuilder.map(keyDetector.schema(), valueDetector.schema()).build();
            }
            var schemaBuilder = SchemaBuilder.struct();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                var newValueDetector = new Values.SchemaDetector();
                if (null == entry.getValue()) {
                    schemaBuilder.field(entry.getKey().toString(), Schema.OPTIONAL_STRING_SCHEMA);
                }
                else if (newValueDetector.canDetect(entry.getValue())) {
                    schemaBuilder.field(entry.getKey().toString(), newValueDetector.schema());
                }
            }
            return schemaBuilder.build();
        }

    }

    @Override
    public Object value() {
        if (null == cachedValue) {
            if (valueSchema() != null && valueSchema().name() != null && valueSchema().name().matches(cloudEventsSchemaNamePattern)) {
                Object originalKafkaValue = originalKafkaRecord.value();

                if (originalKafkaValue instanceof Struct kafkaStruct) {
                    cachedValue = Optional.ofNullable(kafkaStruct.getStruct("data"));
                }
                else if (originalKafkaValue instanceof Map<?, ?> m) {
                    var schemaBuilder = SchemaBuilder.struct();
                    if (Schema.Type.STRUCT.equals(valueSchema().type())) {
                        schemaBuilder.name(valueSchema().field("data").schema().name());
                    }
                    Map<String, ?> mapData = (Map) m.get("data");
                    mapData.forEach((k, v) -> {
                        if (null == v) {
                            // this is a temporary hack to make sure that the schema is valid
                            // TODO: cache the schema and retrieve the field type from the cache / history
                            schemaBuilder.field(k, Schema.OPTIONAL_STRING_SCHEMA);
                        }
                        else {
                            if (v instanceof Map<?, ?> m2) {
                                schemaBuilder.field(k, KafkaValues.inferSchemaFromMap(m2));
                            }
                            else {
                                schemaBuilder.field(k, Values.inferSchema(v));
                            }
                        }
                    });
                    Schema schema = schemaBuilder.build();

                    cachedValue = Optional.of(buildStructFromMap(mapData, schema));
                }
                else {
                    throw new ConnectException("Unexpected value type: " + originalKafkaValue.getClass());
                }
            }
            else if (null == originalKafkaRecord.value()) {
                cachedValue = Optional.of(new Struct(originalKafkaRecord.valueSchema()));
            }
            else {
                cachedValue = Optional.ofNullable(originalKafkaRecord.value());
            }
        }
        return cachedValue.get();
    }

    @Override
    public Schema valueSchema() {
        return originalKafkaRecord.valueSchema();
    }

    @Override
    public boolean isDebeziumMessage() {
        if (null == isDebeziumMessage) {
            if (originalKafkaRecord.value() != null && originalKafkaRecord.valueSchema() != null) {
                Schema kafkaValueSchema = ((Struct) value()).schema();
                isDebeziumMessage = (kafkaValueSchema.name() != null && Envelope.isEnvelopeSchema(kafkaValueSchema.name()))
                        || (null == kafkaValueSchema.name() && null != kafkaValueSchema.field(Envelope.FieldName.OPERATION));
            }
            else {
                isDebeziumMessage = false;
            }
        }
        return isDebeziumMessage;
    }

    @Override
    public boolean isSchemaChange() {
        return originalKafkaRecord.valueSchema() != null
                && !Strings.isNullOrEmpty(originalKafkaRecord.valueSchema().name())
                && originalKafkaRecord.valueSchema().name().contains(SCHEMA_CHANGE_VALUE);
    }

    public boolean isFlattened() {
        if (null == isFlattened) {
            if (!isTombstone() && null != originalKafkaRecord.valueSchema()) {
                Struct value = (Struct) value();
                if (null == value) {
                    isFlattened = true;
                }
                else {
                    isFlattened = value.schema() == null || !isDebeziumMessage();
                }
            }
            else {
                isFlattened = false;
            }
        }
        return isFlattened;
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
        if (null == isDelete) {
            if (!isDebeziumMessage()) {
                this.isDelete = null == originalKafkaRecord.value();
            }
            else if (originalKafkaRecord.value() != null) {
                final Struct value = (Struct) value();
                this.isDelete = Envelope.Operation.DELETE.equals(Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
            }
            else {
                this.isDelete = false;
            }
        }
        return this.isDelete;
    }

    @Override
    public boolean isTruncate() {
        if (isDebeziumMessage()) {
            return Envelope.Operation.TRUNCATE.equals(Envelope.Operation.forCode(((Struct) value()).getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    @Override
    public Struct getPayload() {
        return kafkaPayload;
    }

    @Override
    public Struct getFilteredPayload(FieldFilterFactory.FieldNameFilter fieldsFilter) {
        return filterFields(getPayload(), topicName(), Set.of(), fieldsFilter);
    }

    private Struct filterFields(Struct data, String topicName, Set<String> allowedPrimaryKeyFields, FieldFilterFactory.FieldNameFilter fieldsFilter) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        if (!allowedPrimaryKeyFields.isEmpty()) {
            data.schema().fields().stream()
                    .filter(field -> allowedPrimaryKeyFields.contains(field.name()))
                    .forEach(field -> schemaBuilder.field(field.name(), field.schema()));
            final Schema schema = schemaBuilder.build();
            Struct filteredData = new Struct(schema);
            schema.fields().forEach(field -> filteredData.put(field.name(), data.get(field.name())));
            return filteredData;
        }
        else {
            data.schema().fields().forEach(field -> {
                if (null != fieldsFilter) {
                    if (fieldsFilter.matches(topicName, field.name())) {
                        schemaBuilder.field(field.name(), field.schema());
                    }
                }
                else {
                    schemaBuilder.field(field.name(), field.schema());
                }
            });
            final Schema schema = schemaBuilder.build();
            Struct filteredData = new Struct(schema);
            schema.fields().forEach(field -> {
                Object fieldValue = data.get(field);
                if (null != fieldValue && (null == fieldsFilter || fieldsFilter.matches(topicName, field.name()))) {
                    filteredData.put(field.name(), fieldValue);
                }
            });
            return filteredData;
        }
    }

    @Override
    public Struct getFilteredKey(SinkConnectorConfig.PrimaryKeyMode primaryKeyMode, Set<String> allowedPrimaryKeyFields,
                                 FieldFilterFactory.FieldNameFilter fieldsFilter) {
        switch (primaryKeyMode) {
            case NONE -> {
                return null;
            }
            case RECORD_KEY -> {
                final Schema keySchema = keySchema();
                if (keySchema == null) {
                    throw new ConnectException("Configured primary key mode 'record_key' cannot have null schema");
                }
                else if (keySchema.type().isPrimitive()) {
                    return keyAsStruct();
                }
                if (Schema.Type.STRUCT.equals(keySchema.type())) {
                    return filterFields(keyAsStruct(), topicName(), allowedPrimaryKeyFields, fieldsFilter);
                }
                else {
                    throw new ConnectException("An unsupported record key schema type detected: " + keySchema.type());
                }
            }
            case RECORD_VALUE -> {
                final Struct payload = getPayload();
                final Schema valueSchema = payload.schema();
                if (valueSchema != null && Schema.Type.STRUCT.equals(valueSchema.type())) {
                    return filterFields(payload, topicName(), allowedPrimaryKeyFields, fieldsFilter);
                }
                else {
                    throw new ConnectException("No struct-based primary key defined for record value");
                }
            }
            case RECORD_HEADER -> {
                if (originalKafkaRecord.headers().isEmpty()) {
                    throw new ConnectException("Configured primary key mode 'record_header' cannot have empty message headers");
                }
                return getRecordHeaders();
            }
            case KAFKA -> {
                return kafkaCoordinates();
            }
            default -> throw new DebeziumException("Unknown primary key mode: " + primaryKeyMode);
        }
    }

    public SinkRecord getOriginalKafkaRecord() {
        return originalKafkaRecord;
    }

    private static final String KAFKA_COORDINATES = "__kafka_coordinates";
    private static final String CONNECT_TOPIC = "__connect_topic";
    private static final String CONNECT_PARTITION = "__connect_partition";
    private static final String CONNECT_OFFSET = "__connect_offset";

    public static final Schema KAFKA_COORDINATES_SCHEMA = SchemaBuilder.struct().name(KAFKA_COORDINATES)
            .field(CONNECT_TOPIC, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CONNECT_PARTITION, Schema.OPTIONAL_INT32_SCHEMA)
            .field(CONNECT_OFFSET, Schema.OPTIONAL_INT64_SCHEMA).build();

    private Struct getKafkaCoordinates() {
        Struct kafkaCoordinatesStruct = new Struct(KAFKA_COORDINATES_SCHEMA);
        kafkaCoordinatesStruct
                .put(CONNECT_TOPIC, originalKafkaRecord.topic())
                .put(CONNECT_PARTITION, originalKafkaRecord.kafkaPartition())
                .put(CONNECT_OFFSET, originalKafkaRecord.kafkaOffset());

        kafkaFields.put(CONNECT_TOPIC, new FieldDescriptor(Schema.STRING_SCHEMA, CONNECT_TOPIC, true));
        kafkaFields.put(CONNECT_PARTITION, new FieldDescriptor(Schema.INT32_SCHEMA, CONNECT_PARTITION, true));
        kafkaFields.put(CONNECT_OFFSET, new FieldDescriptor(Schema.INT64_SCHEMA, CONNECT_OFFSET, true));

        return kafkaCoordinatesStruct;
    }

    @Override
    public Map<String, FieldDescriptor> kafkaFields() {
        if (kafkaFields.isEmpty()) {
            getKafkaCoordinates();
        }
        return kafkaFields;
    }

    private Struct keyAsStruct() {
        if (keySchema() != null) {
            return (Struct) originalKafkaRecord.key();
        }
        return null;
    }

    private static final String KAFKA_HEADERS_SUFFIX = "$__kafka_headers";

    private Struct getRecordHeaders() {
        final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct().name(originalKafkaRecord.topic() + KAFKA_HEADERS_SUFFIX);
        originalKafkaRecord.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));
        final Schema headerSchema = headerSchemaBuilder.build();

        final Struct headerStruct = new Struct(headerSchema);
        originalKafkaRecord.headers().forEach((Header header) -> headerStruct.put(header.key(), header.value()));

        return headerStruct;
    }

    private Struct getKafkaPayload(boolean flattened) {
        final Schema valueSchema = valueSchema();
        if (valueSchema == null) {
            return null;
        }
        Struct rawPayload = (Struct) value();
        if (flattened) {
            return Objects.requireNonNullElseGet(rawPayload, () -> new Struct(rawPayload.schema()));
        }
        else {
            if (isDelete()) {
                return rawPayload.getStruct(Envelope.FieldName.BEFORE);
            }
            return rawPayload.getStruct(Envelope.FieldName.AFTER);
        }
    }

}
