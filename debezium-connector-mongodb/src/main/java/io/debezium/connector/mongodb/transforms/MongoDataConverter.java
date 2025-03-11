/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * MongoDataConverter handles translating MongoDB strings to Kafka Connect schemas and row data to Kafka
 * Connect records.
 *
 * @author Sairam Polavarapu
 * @author Anisha Mohanty
 */
public class MongoDataConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConverter.class);
    public static final String SCHEMA_NAME_REGEX = "io.debezium.mongodb.regex";
    private final ArrayEncoding arrayEncoding;
    private final FieldNamer<String> fieldNamer;

    /**
     * Whether to adjust certain field values to conform with Avro requirements.
     */
    private final boolean sanitizeValue;

    public MongoDataConverter(ArrayEncoding arrayEncoding, FieldNamer<String> fieldNamer, boolean sanitizeValue) {
        this.arrayEncoding = arrayEncoding;
        this.fieldNamer = fieldNamer;
        this.sanitizeValue = sanitizeValue;
    }

    public MongoDataConverter(ArrayEncoding arrayEncoding) {
        this(arrayEncoding, FieldNameSelector.defaultNonRelationalSelector(SchemaNameAdjuster.NO_OP), false);
    }

    public Map<String, Map<Object, BsonType>> parseBsonDocument(BsonDocument document) {
        if (document.isEmpty()) {
            return new LinkedHashMap<>();
        }

        Map<String, Map<Object, BsonType>> schemaMap = new LinkedHashMap<>();

        for (Entry<String, BsonValue> entry : document.entrySet()) {
            String key = entry.getKey();
            BsonValue value = entry.getValue();
            BsonType type = value.getBsonType();

            switch (type) {
                case ARRAY:
                    schemaMap.put(key, Map.of(traverseArray(value.asArray()), BsonType.ARRAY));
                    break;
                case DOCUMENT:
                    schemaMap.put(key, Map.of(parseBsonDocument(value.asDocument()), BsonType.DOCUMENT));
                    break;
                default:
                    schemaMap.put(key, Map.of(value, type));
                    break;
            }
        }
        return schemaMap;
    }

    public Map<Object, BsonType> traverseArray(BsonArray array) {
        Map<Object, BsonType> unifiedSchema = new LinkedHashMap<>();
        List<Object> elementsList = new ArrayList<>();
        Map<String, Set<BsonType>> fieldTypeTracker = new LinkedHashMap<>();
        Map<String, Map<Object, BsonType>> mergedDocumentSchema = new LinkedHashMap<>();

        for (BsonValue value : array) {
            BsonType type = value.getBsonType();

            if (type == BsonType.ARRAY) {
                unifiedSchema.put(traverseArray(value.asArray()), BsonType.ARRAY);
            }
            else if (type == BsonType.DOCUMENT) {
                Map<String, Map<Object, BsonType>> documentSchema = parseBsonDocument(value.asDocument());
                mergeSchemas(mergedDocumentSchema, documentSchema, fieldTypeTracker);
            }
            else {
                elementsList.add(value);
            }
        }

        if (!mergedDocumentSchema.isEmpty()) {
            unifiedSchema.put(mergedDocumentSchema, BsonType.DOCUMENT);
        }
        else if (!elementsList.isEmpty()) {
            Set<BsonType> types = new LinkedHashSet<>();
            for (Object element : elementsList) {
                if (element instanceof BsonValue) {
                    types.add(((BsonValue) element).getBsonType());
                }
            }
            unifiedSchema.put(elementsList, types.size() == 1 ? types.iterator().next() : BsonType.ARRAY);
        }

        return unifiedSchema;
    }

    private void mergeSchemas(Map<String, Map<Object, BsonType>> baseSchema,
                              Map<String, Map<Object, BsonType>> newSchema,
                              Map<String, Set<BsonType>> fieldTypeTracker) {
        for (Entry<String, Map<Object, BsonType>> entry : newSchema.entrySet()) {
            String field = entry.getKey();
            Map<Object, BsonType> fieldTypeMap = entry.getValue();
            BsonType newType = fieldTypeMap.values().iterator().next();

            fieldTypeTracker.putIfAbsent(field, new LinkedHashSet<>());
            Set<BsonType> existingTypes = fieldTypeTracker.get(field);

            if (!existingTypes.contains(newType)) {
                existingTypes.add(newType);

                if (baseSchema.containsKey(field)) {
                    Map<Object, BsonType> existingFieldTypeMap = baseSchema.get(field);
                    existingFieldTypeMap.putAll(fieldTypeMap);
                }
                else {
                    baseSchema.put(field, new LinkedHashMap<>(fieldTypeMap));
                }
            }
        }
    }

    public void buildSchema(Map<String, Map<Object, BsonType>> map, SchemaBuilder builder) {
        for (Entry<String, Map<Object, BsonType>> entry : map.entrySet()) {
            String key = fieldNamer.fieldNameFor(entry.getKey());
            for (Entry<Object, BsonType> objectBsonTypeEntry : entry.getValue().entrySet()) {
                schema(key, objectBsonTypeEntry, builder);
            }
        }
    }

    public void schema(String key, Entry<Object, BsonType> entry, SchemaBuilder builder) {
        Object obj = entry.getKey();
        BsonType type = entry.getValue();
        switch (type) {
            case NULL:
            case STRING:
            case JAVASCRIPT:
            case OBJECT_ID:
            case DECIMAL128:
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
                break;
            case DOUBLE:
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
                break;

            case BINARY:
                builder.field(key, Schema.OPTIONAL_BYTES_SCHEMA);
                break;

            case INT32:
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
                break;

            case INT64:
                builder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
                break;

            case DATE_TIME:
            case TIMESTAMP:
                builder.field(key, Timestamp.builder().optional().build());
                break;

            case BOOLEAN:
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                break;

            case JAVASCRIPT_WITH_SCOPE:
                SchemaBuilder jsWithScope = SchemaBuilder.struct().name(builder.name() + "." + key);
                jsWithScope.field("code", Schema.OPTIONAL_STRING_SCHEMA);
                SchemaBuilder scope = SchemaBuilder.struct().name(jsWithScope.name() + "." + key + ".scope").optional();

                for (Entry<?, ?> jwsDoc : ((Map<?, ?>) obj).entrySet()) {
                    String fieldName = fieldNamer.fieldNameFor(jwsDoc.getKey().toString());
                    Object value = jwsDoc.getValue();
                    schema(fieldName, (Entry<Object, BsonType>) value, scope);
                }

                Schema scopeBuild = scope.build();
                jsWithScope.field("scope", scopeBuild).build();
                builder.field(key, jsWithScope);
                break;

            case REGULAR_EXPRESSION:
                SchemaBuilder regexwop = SchemaBuilder.struct().name(SCHEMA_NAME_REGEX).optional();
                regexwop.field("regex", Schema.OPTIONAL_STRING_SCHEMA);
                regexwop.field("options", Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(key, regexwop.build());
                break;

            case DOCUMENT:
                if (obj instanceof Map<?, ?> map) {
                    if (map.isEmpty()) {
                        builder.field(key, SchemaBuilder.struct().optional().build());
                    }
                    else {
                        SchemaBuilder documentBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                        for (Entry<?, ?> doc : map.entrySet()) {
                            String fieldName = fieldNamer.fieldNameFor(doc.getKey().toString());
                            Object value = doc.getValue();

                            if (value instanceof Map<?, ?> nestedMap) {
                                for (Entry<?, ?> nestedDoc : nestedMap.entrySet()) {
                                    schema(fieldName, (Entry<Object, BsonType>) nestedDoc, documentBuilder);
                                }
                            }
                        }
                        builder.field(key, documentBuilder.build());
                    }
                }
                break;

            case ARRAY:
                if ((obj instanceof Map && ((Map<?, ?>) obj).isEmpty())) {
                    if (sanitizeValue) {
                        return;
                    }
                    switch (arrayEncoding) {
                        case ARRAY:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
                            break;
                        case DOCUMENT:
                            builder.field(key, SchemaBuilder.struct().name(builder.name() + "." + key).optional().build());
                            break;
                    }
                }
                else {
                    switch (arrayEncoding) {
                        case ARRAY:
                            SchemaBuilder arrayBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();

                            for (Entry<?, ?> doc : ((Map<?, ?>) obj).entrySet()) {
                                Object object = doc.getKey();

                                if (object instanceof Map<?, ?> map) {
                                    for (Entry<?, ?> entryMap : map.entrySet()) {
                                        String entryMapKey = fieldNamer.fieldNameFor(entryMap.getKey().toString());
                                        Object entryMapValue = entryMap.getValue();

                                        if (entryMapValue instanceof Map<?, ?> nestedMap) {
                                            List<Schema> types = nestedMap.values().stream()
                                                    .map(o -> getSchema(((BsonType) o)))
                                                    .distinct()
                                                    .toList(); // Collecting distinct types

                                            if (types.size() > 1) {
                                                throw new DebeziumException("Array elements with different BSON types are not supported: " + entryMapValue);
                                            }
                                            if (arrayBuilder.field(entryMapKey) == null && !nestedMap.isEmpty()) {
                                                schema(entryMapKey, (Entry<Object, BsonType>) nestedMap.entrySet().iterator().next(), arrayBuilder);
                                            }
                                            else {
                                                for (Entry<?, ?> subEntry : nestedMap.entrySet()) {
                                                    schema(entryMapKey, (Entry<Object, BsonType>) subEntry, arrayBuilder);
                                                }
                                            }
                                        }
                                    }
                                    builder.field(key, SchemaBuilder.array(arrayBuilder.build()).optional().build());
                                }
                                else if (object instanceof List<?> list) {
                                    List<Schema> types = list.stream()
                                            .map(o -> getSchema(((BsonValue) o).getBsonType()))
                                            .distinct()
                                            .toList(); // Collecting distinct types

                                    if (types.size() > 1) {
                                        throw new DebeziumException("Array elements with different BSON types are not supported: " + object);
                                    }

                                    SchemaBuilder arrayListBuilder = SchemaBuilder.array(types.get(0)).optional();
                                    builder.field(key, arrayListBuilder.build());
                                }
                            }
                            break;
                        case DOCUMENT:
                            SchemaBuilder documentBuilder;
                            if (builder.name().endsWith(key)) {
                                documentBuilder = SchemaBuilder.struct().name(builder.name()).optional();
                            }
                            else {
                                documentBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                            }

                            for (Entry<?, ?> doc : ((Map<?, ?>) obj).entrySet()) {
                                Object object = doc.getKey();

                                if (object instanceof List<?> list) {
                                    for (int i = 0; i < list.size(); i++) {
                                        BsonType bsonType = ((BsonValue) list.get(i)).getBsonType();
                                        documentBuilder.field(arrayElementStructName(i), getSchema(bsonType));
                                    }
                                }
                                else if (object instanceof Map<?, ?> map) {
                                    int i = 0;
                                    SchemaBuilder documentMapBuilder = SchemaBuilder.struct().name(documentBuilder.name() + "." + arrayElementStructName(i))
                                            .optional();
                                    SchemaBuilder documentArrayBuilder = null;
                                    for (Entry<?, ?> listMap : map.entrySet()) {
                                        String listMapKey = fieldNamer.fieldNameFor(listMap.getKey().toString());
                                        Object listMapValue = listMap.getValue();
                                        if (listMapValue instanceof Map<?, ?> nestedMap) {
                                            for (Entry<?, ?> subListMap : nestedMap.entrySet()) {
                                                if (nestedMap.size() == 1) {
                                                    if ((subListMap.getKey() instanceof Map && ((Map<?, ?>) subListMap.getKey()).isEmpty())
                                                            && Objects.equals(subListMap.getValue(), BsonType.ARRAY)) {
                                                        // handle empty arrays
                                                        String arrayKey = fieldNamer
                                                                .fieldNameFor(documentMapBuilder.name() + "." + listMapKey);
                                                        documentMapBuilder.field(listMapKey, SchemaBuilder.struct().name(arrayKey).optional().build());
                                                    }
                                                    else if ((subListMap.getKey() instanceof Map && Objects.equals(subListMap.getValue(), BsonType.ARRAY))) {
                                                        // handle nested arrays
                                                        documentArrayBuilder = SchemaBuilder.struct()
                                                                .name(documentMapBuilder.name() + "." + listMapKey).optional();
                                                        schema(listMapKey, (Entry<Object, BsonType>) subListMap, documentArrayBuilder);
                                                        documentMapBuilder.field(listMapKey, documentArrayBuilder.build());
                                                    }
                                                    else {
                                                        BsonValue bsonValue = (BsonValue) subListMap.getKey();
                                                        documentMapBuilder.field(listMapKey, getSchema(bsonValue.getBsonType()));
                                                    }
                                                }
                                                else {
                                                    SchemaBuilder documentElementBuilder = SchemaBuilder.struct()
                                                            .name(builder.name() + "." + key + "." + arrayElementStructName(i)).optional();
                                                    schema(listMapKey, (Entry<Object, BsonType>) subListMap, documentElementBuilder);
                                                    documentBuilder.field(arrayElementStructName(i), documentElementBuilder.build());
                                                    i++;
                                                }
                                            }
                                        }
                                    }
                                    if (!documentMapBuilder.fields().isEmpty()) {
                                        documentBuilder.field(arrayElementStructName(i), documentMapBuilder.build());
                                    }
                                }
                            }
                            if (!documentBuilder.schema().name().equals(builder.schema().name())) {
                                builder.field(key, documentBuilder.build());
                            }
                            else {
                                builder.field(documentBuilder.fields().get(0).name(), documentBuilder.fields().get(0).schema());
                            }
                            break;
                    }
                }
                break;
            default:
                break;
        }
    }

    public Schema getSchema(BsonType type) {
        switch (type) {
            case NULL:
            case STRING:
            case JAVASCRIPT:
            case OBJECT_ID:
            case DECIMAL128:
                return Schema.OPTIONAL_STRING_SCHEMA;

            case DOUBLE:
                return Schema.OPTIONAL_FLOAT64_SCHEMA;

            case BINARY:
                return Schema.OPTIONAL_BYTES_SCHEMA;

            case INT32:
                return Schema.OPTIONAL_INT32_SCHEMA;

            case INT64:
                return Schema.OPTIONAL_INT64_SCHEMA;

            case TIMESTAMP:
            case DATE_TIME:
                return Timestamp.builder().optional().build();

            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;

            case ARRAY:
                switch (arrayEncoding) {
                    case ARRAY:
                        return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
                    case DOCUMENT:
                        return SchemaBuilder.struct().optional().build();
                }

            case DOCUMENT:
                return SchemaBuilder.struct().optional().build();

            default:
                throw new IllegalArgumentException("The value type " + type + " is not supported for sub schema.");
        }
    }

    public void buildStruct(Entry<String, BsonValue> bsonValueEntry, Schema schema, Struct struct) {
        Object colValue = null;
        String key = fieldNamer.fieldNameFor(bsonValueEntry.getKey());
        BsonValue value = bsonValueEntry.getValue();
        BsonType type = value.getBsonType();

        switch (type) {
            case STRING:
                colValue = value.asString().getValue();
                break;

            case OBJECT_ID:
                colValue = value.asObjectId().getValue().toString();
                break;

            case DOUBLE:
                colValue = value.asDouble().getValue();
                break;

            case BINARY:
                colValue = value.asBinary().getData();
                break;

            case INT32:
                colValue = value.asInt32().getValue();
                break;

            case INT64:
                colValue = value.asInt64().getValue();
                break;

            case BOOLEAN:
                colValue = value.asBoolean().getValue();
                break;

            case DATE_TIME:
                colValue = new Date(value.asDateTime().getValue());
                break;

            case TIMESTAMP:
                colValue = new Date(1000L * value.asTimestamp().getTime());
                break;

            case DECIMAL128:
                colValue = value.asDecimal128().getValue().toString();
                break;

            case JAVASCRIPT:
                colValue = value.asJavaScript().getCode();
                break;
            case JAVASCRIPT_WITH_SCOPE:
                Struct jsStruct = new Struct(schema.field(key).schema());
                Struct jsScopeStruct = new Struct(
                        schema.field(key).schema().field("scope").schema());
                jsStruct.put("code", value.asJavaScriptWithScope().getCode());
                BsonDocument jwsDoc = value.asJavaScriptWithScope().getScope().asDocument();

                for (Entry<String, BsonValue> entry : jwsDoc.entrySet()) {
                    buildStruct(entry, schema.field(key).schema().field(key).schema(), jsScopeStruct);
                }

                jsStruct.put("scope", jsScopeStruct);
                colValue = jsStruct;
                break;

            case REGULAR_EXPRESSION:
                Struct regexStruct = new Struct(schema.field(key).schema());
                regexStruct.put("regex", value.asRegularExpression().getPattern());
                regexStruct.put("options", value.asRegularExpression().getOptions());
                colValue = regexStruct;
                break;

            case ARRAY:
                if (value.asArray().isEmpty()) {
                    if (sanitizeValue) {
                        return;
                    }
                    switch (arrayEncoding) {
                        case ARRAY:
                            colValue = new ArrayList<>();
                            break;
                        case DOCUMENT:
                            final Schema fieldSchema = schema.field(key).schema();
                            colValue = new Struct(fieldSchema);
                            break;
                    }
                }
                else {
                    switch (arrayEncoding) {
                        case ARRAY:
                            Schema arraySchema = schema.field(key).schema().type() == Schema.Type.ARRAY
                                    ? schema.field(key).schema().valueSchema()
                                    : schema.field(key).schema();
                            colValue = buildArray(value.asArray(), arraySchema);
                            break;
                        case DOCUMENT:
                            final BsonArray array = value.asArray();
                            final Map<String, BsonValue> convertedArray = new HashMap<>();
                            Schema arraySchemaForDocument = schema.field(key).schema();

                            Struct arrayStruct = new Struct(arraySchemaForDocument);
                            for (int i = 0; i < array.size(); i++) {
                                convertedArray.put(arrayElementStructName(i), array.get(i));
                            }

                            for (Entry<String, BsonValue> entry : convertedArray.entrySet()) {
                                buildStruct(entry, arraySchemaForDocument, arrayStruct);
                            }

                            colValue = arrayStruct;
                            break;
                    }
                }
                break;

            case DOCUMENT:
                Field field = schema.field(key);
                if (field == null) {
                    throw new DebeziumException("Can't find field: " + key + " in schema " + schema);
                }
                Schema documentSchema = field.schema();
                Struct documentStruct = new Struct(documentSchema);
                BsonDocument doc = value.asDocument();

                for (Entry<String, BsonValue> entry : doc.entrySet()) {
                    buildStruct(entry, documentSchema, documentStruct);
                }

                colValue = documentStruct;
                break;

            default:
                break;

        }

        if (type != BsonType.UNDEFINED) {
            struct.put(key, colValue);
        }
    }

    protected String arrayElementStructName(int i) {
        return "_" + i;
    }

    private Object buildArray(BsonArray array, Schema schema) {
        ArrayList<Object> values = new ArrayList<>();
        for (BsonValue value : array) {
            BsonType type = value.getBsonType();
            switch (type) {
                case NULL:
                    values.add(null);
                    break;

                case STRING:
                    values.add(value.asString().getValue());
                    break;

                case OBJECT_ID:
                    values.add(value.asObjectId().getValue().toString());
                    break;

                case DECIMAL128:
                    values.add(value.asDecimal128().getValue().toString());
                    break;

                case DOUBLE:
                    values.add(value.asDouble().getValue());
                    break;

                case BINARY:
                    values.add(value.asBinary().getData());
                    break;

                case INT32:
                    values.add(value.asInt32().getValue());
                    break;

                case INT64:
                    values.add(value.asInt64().getValue());
                    break;

                case BOOLEAN:
                    values.add(value.asBoolean().getValue());
                    break;

                case DATE_TIME:
                    values.add(new Date(value.asDateTime().getValue()));
                    break;

                case JAVASCRIPT:
                    values.add(value.asJavaScript().getCode());
                    break;

                case ARRAY:
                    values.add(buildArray(value.asArray(), schema.valueSchema()));
                    break;

                case DOCUMENT:
                    BsonDocument doc = value.asDocument();
                    Struct struct = new Struct(schema);
                    for (Entry<String, BsonValue> entry : doc.entrySet()) {
                        buildStruct(entry, schema, struct);
                    }
                    values.add(struct);
                    break;
            }
        }
        return values;
    }
}
