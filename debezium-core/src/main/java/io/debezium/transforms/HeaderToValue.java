/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.transforms.HeaderToValue.Operation.MOVE;
import static java.lang.String.format;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.BoundedConcurrentHashMap;

public class HeaderToValue<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderToValue.class);
    public static final String FIELDS_CONF = "fields";
    public static final String HEADERS_CONF = "headers";
    public static final String OPERATION_CONF = "operation";
    private static final String MOVE_OPERATION = "move";
    private static final String COPY_OPERATION = "copy";
    private static final int CACHE_SIZE = 64;
    public static final String NESTING_SEPARATOR = ".";
    public static final String ROOT_FIELD_NAME = "payload";
    public static final String EMPTY_STRING = "";
    public static final String SPACE = " ";

    enum Operation {
        MOVE(MOVE_OPERATION),
        COPY(COPY_OPERATION);

        private final String name;

        Operation(String name) {
            this.name = name;
        }

        static Operation fromName(String name) {
            switch (name) {
                case MOVE_OPERATION:
                    return MOVE;
                case COPY_OPERATION:
                    return COPY;
                default:
                    throw new IllegalArgumentException();
            }
        }

        public String toString() {
            return name;
        }
    }

    public static final Field HEADERS_FIELD = Field.create(HEADERS_CONF)
            .withDisplayName("Header names list")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Header names in the record whose values are to be copied or moved to record value.")
            .required();

    public static final Field FIELDS_FIELD = Field.create(FIELDS_CONF)
            .withDisplayName("Field names list")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Field names, in the same order as the header names listed in the headers configuration property. Supports Struct nesting using dot notation.")
            .required();

    public static final Field OPERATION_FIELD = Field.create(OPERATION_CONF)
            .withDisplayName("Operation: mover or copy")
            .withType(ConfigDef.Type.STRING)
            .withEnum(Operation.class)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Either <code>move</code> if the fields are to be moved to the value (removed from the headers), " +
                    "or <code>copy</code> if the fields are to be copied to the value (retained in the headers).")
            .required();

    private List<String> fields;

    private List<String> headers;

    private Operation operation;

    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);
    private final BoundedConcurrentHashMap<Headers, Headers> headersUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    @Override
    public ConfigDef config() {

        final ConfigDef config = new ConfigDef();
        Field.group(config, null, HEADERS_FIELD, FIELDS_FIELD, OPERATION_FIELD);
        return config;
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELDS_FIELD, HEADERS_FIELD, OPERATION_FIELD));

        fields = config.getList(FIELDS_FIELD);
        headers = config.getList(HEADERS_FIELD);

        validateConfiguration();

        operation = Operation.fromName(config.getString(OPERATION_FIELD));
    }

    private void validateConfiguration() {

        if (headers.size() != fields.size()) {
            throw new ConfigException(format("'%s' config must have the same number of elements as '%s' config.",
                    FIELDS_FIELD, HEADERS_FIELD));
        }

        if (headers.contains(EMPTY_STRING) || fields.contains(EMPTY_STRING)) {
            throw new ConfigException(format("'%s' and/or '%s' config contains a not valid empty string.",
                    FIELDS_FIELD, HEADERS_FIELD));
        }

        if (headers.stream().anyMatch(h -> h.contains(SPACE))) {
            throw new ConfigException(format("'%s' config contains a field with a not valid space.",
                    HEADERS_FIELD));
        }

        if (fields.stream().anyMatch(f -> f.contains(SPACE))) {
            throw new ConfigException(format("'%s' config contains a field with a not valid space.",
                    FIELDS_CONF));
        }
    }

    @Override
    public R apply(R record) {

        final Struct value = requireStruct(record.value(), "Header field insertion");

        Map<String, Header> headerToProcess = StreamSupport.stream(record.headers().spliterator(), false)
                .filter(header -> headers.contains(header.key()))
                .collect(Collectors.toMap(Header::key, Function.identity()));

        LOGGER.trace("Header to be processed: {}", print(headerToProcess));

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), valueSchema -> makeNewSchema(valueSchema, headerToProcess));

        LOGGER.trace("Updated schema: {}", updatedSchema);

        Struct updatedValue = makeUpdatedValue(value, headerToProcess, updatedSchema);

        LOGGER.trace("Updated value: {}", updatedValue);

        Headers updatedHeaders = record.headers();
        if (MOVE.equals(operation)) {
            updatedHeaders = headersUpdateCache.computeIfAbsent(record.headers(), this::removeHeaders);
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp(),
                updatedHeaders);
    }

    private Headers removeHeaders(Headers originalHeaders) {

        Headers updatedHeaders = originalHeaders.duplicate();
        headers.forEach(updatedHeaders::remove);

        return updatedHeaders;
    }

    private Struct makeUpdatedValue(Struct originalValue, Map<String, Header> headerToProcess, Schema updatedSchema) {

        List<String> nestedFields = fields.stream().filter(field -> field.contains(NESTING_SEPARATOR)).collect(Collectors.toList());

        return buildUpdatedValue(ROOT_FIELD_NAME, originalValue, headerToProcess, updatedSchema, nestedFields, 0);
    }

    private Struct buildUpdatedValue(String fieldName, Struct originalValue, Map<String, Header> headerToProcess, Schema updatedSchema, List<String> nestedFields,
                                     int level) {

        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : originalValue.schema().fields()) {
            if (originalValue.get(field) != null) {
                if (isContainedIn(field.name(), nestedFields)) {
                    Struct nestedField = requireStruct(originalValue.get(field), "Nested field");
                    updatedValue.put(field.name(),
                            buildUpdatedValue(field.name(), nestedField, headerToProcess, updatedSchema.field(field.name()).schema(), nestedFields, ++level));
                }
                else {
                    updatedValue.put(field.name(), originalValue.get(field));
                }
            }
        }

        for (int i = 0; i < headers.size(); i++) {

            Header currentHeader = headerToProcess.get(headers.get(i));

            if (currentHeader != null) {
                Optional<String> fieldNameToAdd = getFieldName(fields.get(i), fieldName, level);
                fieldNameToAdd.ifPresent(s -> updatedValue.put(s, currentHeader.value()));
            }
        }

        return updatedValue;
    }

    private boolean isContainedIn(String fieldName, List<String> nestedFields) {

        return nestedFields.stream().anyMatch(s -> s.contains(fieldName));
    }

    private Schema makeNewSchema(Schema oldSchema, Map<String, Header> headerToProcess) {

        List<String> nestedFields = fields.stream().filter(field -> field.contains(NESTING_SEPARATOR)).collect(Collectors.toList());

        return buildNewSchema(ROOT_FIELD_NAME, oldSchema, headerToProcess, nestedFields, 0);
    }

    private Schema buildNewSchema(String fieldName, Schema oldSchema, Map<String, Header> headerToProcess, List<String> nestedFields, int level) {

        if (oldSchema.type().isPrimitive()) {
            return oldSchema;
        }

        // Get fields from original schema
        SchemaBuilder newSchemabuilder = SchemaUtil.copySchemaBasics(oldSchema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : oldSchema.fields()) {
            if (isContainedIn(field.name(), nestedFields)) {

                newSchemabuilder.field(field.name(), buildNewSchema(field.name(), field.schema(), headerToProcess, nestedFields, ++level));
            }
            else {
                newSchemabuilder.field(field.name(), field.schema());
            }
        }

        for (int i = 0; i < headers.size(); i++) {

            Header currentHeader = headerToProcess.get(headers.get(i));
            Optional<String> currentFieldName = getFieldName(fields.get(i), fieldName, level);

            if (currentFieldName.isPresent() && currentHeader != null) {
                newSchemabuilder = newSchemabuilder.field(currentFieldName.get(), currentHeader.schema());
            }
        }

        return newSchemabuilder.build();
    }

    private Optional<String> getFieldName(String destinationFieldName, String fieldName, int level) {

        String[] nestedNames = destinationFieldName.split("\\.");
        if (isRootField(fieldName, nestedNames)) {
            return Optional.of(nestedNames[0]);
        }

        if (isChildrenOf(fieldName, level, nestedNames)) {
            return Optional.of(nestedNames[level]);
        }

        return Optional.empty();
    }

    private static boolean isChildrenOf(String fieldName, int level, String[] nestedNames) {
        int parentLevel = level == 0 ? 0 : level - 1;
        return nestedNames[parentLevel].equals(fieldName);
    }

    private static boolean isRootField(String fieldName, String[] nestedNames) {
        return nestedNames.length == 1 && fieldName.equals(ROOT_FIELD_NAME);
    }

    private String print(Map<?, ?> map) {
        return map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public void close() {
    }
}
