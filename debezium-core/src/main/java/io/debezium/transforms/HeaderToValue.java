/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.BoundedConcurrentHashMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class HeaderToValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_CONF = "fields";
    public static final String HEADERS_CONF = "headers";
    public static final String OPERATION_CONF = "operation";
    private static final String MOVE_OPERATION = "move";
    private static final String COPY_OPERATION = "copy";
    private static final int SCHEMA_CACHE_SIZE = 64;

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
            .withDescription("Header names in the record whose values are to be copied or moved to value.")
            .required();

    public static final Field FIELDS_FIELD = Field.create(FIELDS_CONF)
            .withDisplayName("Field names list")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Field names, in the same order as the header names listed in the headers configuration property. Supports Struct nesting using dot notation.")
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

    private SmtManager<R> smtManager;

    private BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
    @Override
    public ConfigDef config() {

        final ConfigDef config = new ConfigDef();
        Field.group(config, null, HEADERS_FIELD, FIELDS_FIELD, OPERATION_FIELD);
        return config;
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELDS_FIELD, HEADERS_FIELD, OPERATION_FIELD));

        fields = config.getList(FIELDS_FIELD);
        headers = config.getList(HEADERS_FIELD);
        if (headers.size() != fields.size()) {
            throw new ConfigException(format("'%s' config must have the same number of elements as '%s' config.",
                    FIELDS_FIELD, HEADERS_FIELD));
        }
        operation = Operation.fromName(config.getString(OPERATION_FIELD));
    }

    @Override
    public R apply(R record) {

        final Struct value = requireStruct(record.value(), "");

        Map<String, Header> headerToProcess = StreamSupport.stream(record.headers().spliterator(), false)
                .filter(header -> headers.contains(header.key()))
                .collect(Collectors.toMap(Header::key, Function.identity()));

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
                s -> makeNewSchema(value.schema(), headerToProcess));

        // Update the value with the new fields
        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            if (value.get(field) != null) {
                updatedValue.put(field.name(), value.get(field));
            }
        }
        for (int i = 0; i < headers.size(); i++) {

            Header currentHeader = headerToProcess.get(headers.get(i));

            updatedValue.put(fields.get(i), currentHeader.value());
        }


        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp(),
                record.headers());
    }

    private Schema makeNewSchema(Schema oldSchema, Map<String, Header> headerToProcess) {

        // Get fields from original schema
        SchemaBuilder newSchemabuilder = SchemaUtil.copySchemaBasics(oldSchema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : oldSchema.fields()) {
            newSchemabuilder.field(field.name(), field.schema());
        }

        for (int i = 0; i < headers.size(); i++) {

            Header currentHeader = headerToProcess.get(headers.get(i));
            String destinationFieldName = fields.get(i);
            newSchemabuilder = newSchemabuilder.field(destinationFieldName, currentHeader.schema());
        }

        return newSchemabuilder.build();
    }

    @Override
    public void close() {

    }
}
