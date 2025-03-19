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

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Loggings;

/**
 * A set of utilities for more easily creating various kinds of transformations.
 */
public class ConnectRecordUtil {

    private static final String UPDATE_DESCRIPTION = "updateDescription";
    public static final String NESTING_SEPARATOR = ".";
    public static final String ROOT_FIELD_NAME = "payload";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectRecordUtil.class);

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

    public static Struct makeUpdatedValue(List<String> fields, List<String> headers, Struct originalValue, Map<String, Header> headerToProcess, Schema updatedSchema) {

        List<String> nestedFields = fields.stream().filter(field -> field.contains(NESTING_SEPARATOR)).collect(Collectors.toList());

        return buildUpdatedValue(fields, headers, ROOT_FIELD_NAME, originalValue, headerToProcess, updatedSchema, nestedFields, 0);
    }

    private static Struct buildUpdatedValue(List<String> fields, List<String> headers, String fieldName, Struct originalValue, Map<String, Header> headerToProcess,
                                            Schema updatedSchema, List<String> nestedFields,
                                            int level) {

        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : originalValue.schema().fields()) {
            if (originalValue.get(field) != null) {
                if (isContainedIn(field.name(), nestedFields)) {
                    Struct nestedField = requireStruct(originalValue.get(field), "Nested field");
                    updatedValue.put(field.name(),
                            buildUpdatedValue(fields, headers, field.name(), nestedField, headerToProcess, updatedSchema.field(field.name()).schema(), nestedFields,
                                    ++level));
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

    public static Schema makeNewSchema(List<String> fields, List<String> headers, Schema oldSchema, Map<String, Header> headerToProcess) {

        List<String> nestedFields = fields.stream().filter(field -> field.contains(NESTING_SEPARATOR)).collect(Collectors.toList());

        return buildNewSchema(fields, headers, ROOT_FIELD_NAME, oldSchema, headerToProcess, nestedFields, 0);
    }

    private static Schema buildNewSchema(List<String> fields, List<String> headers, String fieldName, Schema oldSchema, Map<String, Header> headerToProcess,
                                         List<String> nestedFields, int level) {

        if (oldSchema.type().isPrimitive()) {
            return oldSchema;
        }

        // Get fields from original schema
        SchemaBuilder newSchemabuilder = SchemaUtil.copySchemaBasics(oldSchema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : oldSchema.fields()) {
            if (isContainedIn(field.name(), nestedFields)) {

                newSchemabuilder.field(field.name(), buildNewSchema(fields, headers, field.name(), field.schema(), headerToProcess, nestedFields, ++level));
            }
            else {
                newSchemabuilder.field(field.name(), field.schema());
            }
        }

        LOGGER.debug("Fields copied from the old schema {}", newSchemabuilder.fields());
        for (int i = 0; i < headers.size(); i++) {

            Header currentHeader = headerToProcess.get(headers.get(i));
            Optional<String> currentFieldName = getFieldName(fields.get(i), fieldName, level);
            Loggings.logTraceAndTraceRecord(LOGGER, List.of(headers.get(i), currentFieldName), "CurrentHeader and currentFieldName");
            if (currentFieldName.isPresent() && currentHeader != null) {
                newSchemabuilder = newSchemabuilder.field(currentFieldName.get(), currentHeader.schema());
            }
        }
        LOGGER.debug("Fields added from headers {}", newSchemabuilder.fields());
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
        return nestedFields.stream().anyMatch(s -> s.contains(fieldName));
    }

    private static boolean isChildrenOf(String fieldName, int level, String[] nestedNames) {
        int parentLevel = level == 0 ? 0 : level - 1;
        return nestedNames[parentLevel].equals(fieldName);
    }

    private static boolean isRootField(String fieldName, String[] nestedNames) {
        return nestedNames.length == 1 && fieldName.equals(ROOT_FIELD_NAME);
    }
}
