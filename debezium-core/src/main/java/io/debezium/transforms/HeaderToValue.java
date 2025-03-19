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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Loggings;

public class HeaderToValue<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderToValue.class);
    public static final String FIELDS_CONF = "fields";
    public static final String HEADERS_CONF = "headers";
    public static final String OPERATION_CONF = "operation";
    private static final String MOVE_OPERATION = "move";
    private static final String COPY_OPERATION = "copy";
    private static final int CACHE_SIZE = 64;

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
            .withValidation(
                    Field::notContainSpaceInAnyElement,
                    Field::notContainEmptyElements)
            .withDescription("Header names in the record whose values are to be copied or moved to record value.")
            .required();

    public static final Field FIELDS_FIELD = Field.create(FIELDS_CONF)
            .withDisplayName("Field names list")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    Field::notContainSpaceInAnyElement,
                    Field::notContainEmptyElements)
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
    }

    @Override
    public R apply(R record) {

        if (record.value() == null) {
            Loggings.logTraceAndTraceRecord(LOGGER, record.key(), "Tombstone record arrived and will be skipped");
            return record;
        }

        final Struct value = requireStruct(record.value(), "Header field insertion");

        Loggings.logTraceAndTraceRecord(LOGGER, value, "Processing record");
        Map<String, Header> headerToProcess = StreamSupport.stream(record.headers().spliterator(), false)
                .filter(header -> headers.contains(header.key()))
                .collect(Collectors.toMap(Header::key, Function.identity()));

        if (LOGGER.isTraceEnabled()) {
            Loggings.logTraceAndTraceRecord(LOGGER, headersToString(headerToProcess), "Header to be processed");
        }

        if (headerToProcess.isEmpty()) {
            return record;
        }

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
                valueSchema -> ConnectRecordUtil.makeNewSchema(fields, headers, valueSchema, headerToProcess));

        LOGGER.trace("Updated schema fields: {}", updatedSchema.fields());

        Struct updatedValue = ConnectRecordUtil.makeUpdatedValue(fields, headers, value, headerToProcess, updatedSchema);

        Loggings.logTraceAndTraceRecord(LOGGER, updatedValue, "Updated value");

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

    private String headersToString(Map<?, ?> map) {
        return map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
