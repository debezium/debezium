/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utilities for more easily creating various kinds of transformations.
 */
public class ConnectRecordUtil {

    private static final String UPDATE_DESCRIPTION = "updateDescription";
    public static final String NESTING_SEPARATOR = ".";
    public static final String ROOT_FIELD_NAME = "payload";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectRecordUtil.class);

    public record NewEntry(String name, Schema schema, Object value) {
    };

    public static <R extends ConnectRecord<R>> ExtractField<R> extractAfterDelegate() {
        return extractValueDelegate(AFTER);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractBeforeDelegate() {
        return extractValueDelegate(BEFORE);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractUpdateDescriptionDelegate() {
        return extractValueDelegate(UPDATE_DESCRIPTION);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractValueDelegate(String field) {
        ExtractField<R> extractField = new ExtractField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", field);
        extractField.configure(delegateConfig);
        return extractField;
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractKeyDelegate(String field) {
        ExtractField<R> extractField = new ExtractField.Key<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", field);
        extractField.configure(delegateConfig);
        return extractField;
    }

    public static <R extends ConnectRecord<R>> InsertField<R> insertStaticValueDelegate(String field, String value, boolean replaceNullWithDefault) {
        InsertField<R> insertDelegate = new InsertField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", field);
        delegateConfig.put("static.value", value);
        delegateConfig.put("replace.null.with.default", replaceNullWithDefault ? "true" : "false");
        insertDelegate.configure(delegateConfig);
        return insertDelegate;
    }

    /**
     * Returns a Transformation that sets the record's value and schema to null.
     */
    public static <R extends ConnectRecord<R>> Transformation<R> convertToTombstoneDelegate() {
        return new Transformation<R>() {
            @Override
            public R apply(R record) {
                LOGGER.info("Applying transformation to convert to tombstone");
                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        null,
                        null,
                        record.timestamp());
            }

            @Override
            public ConfigDef config() {
                return new ConfigDef();
            }

            @Override
            public void configure(Map<String, ?> configs) {
                // No configuration required
            }

            @Override
            public void close() {
                // No cleanup required
            }
        };
    }

    public static <R extends ConnectRecord<R>> ReplaceField<R> dropFieldFromValueDelegate(String field) {
        ReplaceField<R> dropFieldDelegate = new ReplaceField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("exclude", field);
        dropFieldDelegate.configure(delegateConfig);
        return dropFieldDelegate;
    }

    public static <R extends ConnectRecord<R>> Flatten<R> flattenValueDelegate(String delimiter) {
        Flatten<R> recordFlattener = new Flatten.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("delimiter", delimiter);
        recordFlattener.configure(delegateConfig);
        return recordFlattener;
    }

    public static Struct makeUpdatedValue(Struct originalValue, List<NewEntry> newEntries, Schema updatedSchema) {
        List<String> nestedFields = newEntries.stream().filter(e -> e.name().contains(NESTING_SEPARATOR)).map(e -> e.name()).collect(Collectors.toList());
        return buildUpdatedValue(ROOT_FIELD_NAME, originalValue, newEntries, updatedSchema, nestedFields, 0);
    }

    private static Struct buildUpdatedValue(String fieldName, Struct originalValue, List<NewEntry> newEntries, Schema updatedSchema, List<String> nestedFields,
                                            int level) {
        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : originalValue.schema().fields()) {
            if (originalValue.get(field) != null) {
                if (isContainedIn(field.name(), nestedFields)) {
                    Struct nestedField = requireStruct(originalValue.get(field), "Nested field");
                    updatedValue.put(field.name(),
                            buildUpdatedValue(field.name(), nestedField, newEntries, updatedSchema.field(field.name()).schema(), nestedFields, level + 1));
                }
                else {
                    updatedValue.put(field.name(), originalValue.get(field));
                }
            }
        }

        for (NewEntry entry : newEntries) {
            Optional<String> fieldNameToAdd = getFieldName(entry.name(), fieldName, level);
            fieldNameToAdd.ifPresent(s -> updatedValue.put(s, entry.value()));
        }

        return updatedValue;
    }

    public static Schema makeNewSchema(Schema oldSchema, List<NewEntry> newEntries) {
        List<String> nestedFields = newEntries.stream().filter(e -> e.name().contains(NESTING_SEPARATOR)).map(e -> e.name()).collect(Collectors.toList());
        return buildNewSchema(ROOT_FIELD_NAME, oldSchema, newEntries, nestedFields, 0);
    }

    private static Schema buildNewSchema(String fieldName, Schema oldSchema, List<NewEntry> newEntries, List<String> nestedFields, int level) {
        if (oldSchema.type().isPrimitive()) {
            return oldSchema;
        }

        // Get fields from original schema
        SchemaBuilder newSchemabuilder = SchemaUtil.copySchemaBasics(oldSchema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : oldSchema.fields()) {
            if (isContainedIn(field.name(), nestedFields)) {
                newSchemabuilder.field(field.name(), buildNewSchema(field.name(), field.schema(), newEntries, nestedFields, level + 1));
            }
            else {
                newSchemabuilder.field(field.name(), field.schema());
            }
        }

        LOGGER.debug("Fields copied from the old schema {}", newSchemabuilder.fields());
        for (NewEntry entry : newEntries) {
            Optional<String> currentFieldName = getFieldName(entry.name(), fieldName, level);
            if (currentFieldName.isPresent()) {
                newSchemabuilder = newSchemabuilder.field(currentFieldName.get(), entry.schema());
            }
        }
        LOGGER.debug("Newly added fields {}", newSchemabuilder.fields());
        return newSchemabuilder.build();
    }

    private static Optional<String> getFieldName(String destinationFieldName, String fieldName, int level) {
        String[] nestedNames = destinationFieldName.split("\\.");
        if (isRootField(fieldName, nestedNames)) {
            return Optional.of(nestedNames[0]);
        }

        if (isChildrenOf(fieldName, level, nestedNames)) {
            return Optional.of(nestedNames[level]);
        }

        return Optional.empty();
    }

    private static boolean isContainedIn(String fieldName, List<String> nestedFields) {
        final String fieldNameWithSeparator = fieldName + NESTING_SEPARATOR;
        return nestedFields.stream().anyMatch(s -> s.contains(fieldNameWithSeparator));
    }

    private static boolean isChildrenOf(String fieldName, int level, String[] nestedNames) {
        if (nestedNames.length != (level + 1)) {
            return false;
        }
        int parentLevel = level == 0 ? 0 : level - 1;
        return nestedNames[parentLevel].equals(fieldName);
    }

    private static boolean isRootField(String fieldName, String[] nestedNames) {
        return nestedNames.length == 1 && fieldName.equals(ROOT_FIELD_NAME);
    }
}
