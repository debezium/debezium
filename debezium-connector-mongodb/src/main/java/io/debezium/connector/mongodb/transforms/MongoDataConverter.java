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

    /**
     * Parses a BsonDocument and builds a schema map
     *
     * @param document the BsonDocument to parse
     * @return a map representing the schema of the document
     */
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

    /**
     * Traverses a BsonArray and builds a unified schema for the array elements
     *
     * @param array the BsonArray to traverse
     * @return a map representing the unified schema of the array elements
     */
    public Map<Object, BsonType> traverseArray(BsonArray array) {
        Map<Object, BsonType> unifiedSchema = new LinkedHashMap<>();
        List<Object> arrayElementList = new ArrayList<>();
        Map<String, Set<Schema>> fieldSchemas = new LinkedHashMap<>();
        Map<String, Map<Object, BsonType>> mergedDocumentSchema = new LinkedHashMap<>();

        for (BsonValue value : array) {
            BsonType type = value.getBsonType();

            switch (type) {
                case ARRAY:
                    unifiedSchema.put(traverseArray(value.asArray()), BsonType.ARRAY);
                    break;
                case DOCUMENT:
                    Map<String, Map<Object, BsonType>> documentSchema = parseBsonDocument(value.asDocument());
                    switch (arrayEncoding) {
                        case ARRAY:
                            mergeSchemas(mergedDocumentSchema, documentSchema, fieldSchemas);
                            break;
                        case DOCUMENT:
                            unifiedSchema.put(documentSchema, BsonType.DOCUMENT);
                            break;
                    }
                    break;
                default:
                    arrayElementList.add(value);
                    break;
            }
        }

        if (arrayEncoding == ArrayEncoding.ARRAY && !mergedDocumentSchema.isEmpty()) {
            unifiedSchema.put(mergedDocumentSchema, BsonType.DOCUMENT);
        }
        else if (!arrayElementList.isEmpty()) {
            Set<BsonType> types = new LinkedHashSet<>();
            for (Object element : arrayElementList) {
                if (element instanceof BsonValue) {
                    types.add(((BsonValue) element).getBsonType());
                }
            }
            unifiedSchema.put(arrayElementList, types.size() == 1 ? types.iterator().next() : BsonType.ARRAY);
        }
        return unifiedSchema;
    }

    /**
     * Checks if a value is a nested structure (either a map or a list)
     */
    private boolean isNestedMapObject(Object value) {
        return value instanceof Map && !((Map<?, ?>) value).isEmpty();
    }

    /**
     * Checks if a value is an empty nested structure
     */
    private boolean isEmptyNestedMapObject(Object value) {
        return value instanceof Map && ((Map<?, ?>) value).isEmpty();
    }

    /**
     * Checks if a value is a nested list structure
     */
    private boolean isNestedListObject(Object value) {
        return value instanceof List && !((List<?>) value).isEmpty();
    }

    /**
     * Checks if a value is the same as a BsonType
     */
    private boolean isSameType(Object value, BsonType type) {
        return Objects.equals(value, type);
    }

    /**
     * Merges two schemas into a base schema only for ARRAY encoding
     *
     * @param baseDocument   the base document schema
     * @param subDocument    the sub document schema to merge
     * @param fieldSchemas   the field schemas to update
     */
    private void mergeSchemas(Map<String, Map<Object, BsonType>> baseDocument,
                              Map<String, Map<Object, BsonType>> subDocument,
                              Map<String, Set<Schema>> fieldSchemas) {

        for (Entry<String, Map<Object, BsonType>> entry : subDocument.entrySet()) {
            String field = entry.getKey();
            Map<Object, BsonType> newValue = entry.getValue();
            BsonType type = newValue.values().iterator().next();
            Schema schema = getSchema(type);
            Object value = newValue.keySet().iterator().next();

            fieldSchemas.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(schema);

            if (isNestedMapObject(value) && type == BsonType.DOCUMENT) {
                baseDocument.merge(field, new LinkedHashMap<>(newValue), (existing, incoming) -> existing.equals(value) ? existing : incoming);
            }
            else {
                if (fieldSchemas.get(field).size() > 1) {
                    throw new DebeziumException("Array elements with different Bson types are not supported: " + fieldSchemas);
                }
                else {
                    baseDocument.put(field, new LinkedHashMap<>(newValue));
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

    /**
     * Builds the schema for a given key and entry
     *
     * @param key    the key to build the schema for
     * @param entry  the entry containing the value and its BsonType
     * @param builder the SchemaBuilder to build the schema
     */
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
                if (!isNestedMapObject(obj)) {
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
                            for (Entry<?, ?> doc : ((Map<?, ?>) obj).entrySet()) {
                                Object object = doc.getKey();

                                if (object instanceof Map<?, ?> map) {
                                    SchemaBuilder nestedBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                                    handleNestedArray(key, map, builder, nestedBuilder, false);
                                }
                                else if (object instanceof List<?> list) {
                                    handleSimpleArray(key, list, builder, false);
                                }
                            }
                            break;
                        case DOCUMENT:
                            SchemaBuilder documentBuilder = createDocumentBuilder(builder, key);
                            int fieldIndex = 0;
                            for (Entry<?, ?> doc : ((Map<?, ?>) obj).entrySet()) {
                                Object object = doc.getKey();

                                if (isNestedListObject(object)) {
                                    parseArrayLists(documentBuilder, (List<?>) object);
                                }
                                else if (isNestedMapObject(object)) {
                                    SchemaBuilder documentMapBuilder = SchemaBuilder.struct()
                                            .name(documentBuilder.name() + "." + arrayElementStructName(fieldIndex)).optional();

                                    parseMapLists(documentMapBuilder, documentBuilder, (Map<?, ?>) object, builder.name() + "." + key, fieldIndex);

                                    if (!documentMapBuilder.fields().isEmpty()) {
                                        documentBuilder.field(arrayElementStructName(fieldIndex++), documentMapBuilder.build());
                                    }
                                }
                            }

                            if (!documentBuilder.schema().name().equals(builder.schema().name())) {
                                builder.field(key, documentBuilder.build());
                            }
                            else {
                                for (Field field : documentBuilder.fields()) {
                                    builder.field(field.name(), field.schema());
                                }
                            }
                            break;
                    }
                }
                break;
            default:
                break;
        }
    }

    /**
     * Parses a map of array objects and adds them to the document builder
     */
    private void parseMapLists(SchemaBuilder documentMapBuilder, SchemaBuilder documentBuilder, Map<?, ?> map, String parentKey, int fieldIndex) {
        for (Entry<?, ?> entry : map.entrySet()) {

            String entryKey = fieldNamer.fieldNameFor(entry.getKey().toString());
            Object entryValue = entry.getValue();

            if (isNestedMapObject(entryValue)) {
                for (Entry<?, ?> subEntry : ((Map<?, ?>) entryValue).entrySet()) {

                    if (((Map<?, ?>) entryValue).size() == 1) {
                        if (isEmptyNestedMapObject(subEntry.getKey()) && isSameType(subEntry.getValue(), BsonType.ARRAY)) {
                            // parse empty arrays
                            String arrayKey = fieldNamer.fieldNameFor(documentMapBuilder.name() + "." + entryKey);
                            documentMapBuilder.field(entryKey, SchemaBuilder.struct().name(arrayKey).optional().build());
                        }
                        else if (isNestedMapObject(subEntry.getKey()) && isSameType(subEntry.getValue(), BsonType.ARRAY)) {
                            // parse nested arrays
                            SchemaBuilder documentArrayBuilder = SchemaBuilder.struct()
                                    .name(documentMapBuilder.name() + "." + entryKey).optional();
                            schema(entryKey, (Entry<Object, BsonType>) subEntry, documentArrayBuilder);
                            documentMapBuilder.field(entryKey, documentArrayBuilder.build());
                        }
                        else {
                            // parse default values
                            BsonValue bsonValue = (BsonValue) subEntry.getKey();
                            documentMapBuilder.field(entryKey, getSchema(bsonValue.getBsonType()));
                        }
                    }
                    else {
                        SchemaBuilder documentElementBuilder = SchemaBuilder.struct()
                                .name(parentKey + "." + arrayElementStructName(fieldIndex)).optional();
                        schema(entryKey, (Entry<Object, BsonType>) subEntry, documentElementBuilder);
                        documentBuilder.field(arrayElementStructName(fieldIndex++), documentElementBuilder.build());
                    }
                }
            }
            else if (isNestedListObject(entry.getKey())) {
                parseArrayLists(documentMapBuilder, (List<?>) entry.getKey());
            }
        }
    }

    /**
     * Parses list of array objects and adds them to the document builder
     */
    private void parseArrayLists(SchemaBuilder documentMapBuilder, List<?> list) {
        for (int index = 0; index < list.size(); index++) {
            if (list.get(index) instanceof BsonValue bsonValue) {
                documentMapBuilder.field(arrayElementStructName(index), getSchema(bsonValue.getBsonType()));
            }
        }
    }

    /**
     * Creates a document builder with appropriate name based on the parent builder and key
     */
    private SchemaBuilder createDocumentBuilder(SchemaBuilder parentBuilder, String key) {
        if (parentBuilder.name().endsWith(key)) {
            return SchemaBuilder.struct().name(parentBuilder.name()).optional();
        }

        return SchemaBuilder.struct().name(parentBuilder.name() + "." + key).optional();
    }

    /**
     * Handles nested arrays and arrays of documents in the schema
     */
    private void handleNestedArray(String key, Map<?, ?> map, SchemaBuilder builder, SchemaBuilder nestedBuilder, boolean nested) {
        for (Entry<?, ?> doc : map.entrySet()) {
            Object docKey = doc.getKey();
            Object docValue = doc.getValue();

            if (docKey instanceof List<?> list) {
                handleSimpleArray(key, list, builder, true);
            }
            else if (docKey instanceof Map<?, ?> nestedMap) {
                if (docValue.equals(BsonType.ARRAY)) {
                    handleSimpleArray(key, new ArrayList<>(nestedMap.values()), builder, true);
                }
                else if (docValue.equals(BsonType.DOCUMENT)) {
                    SchemaBuilder documentArrayBuilder = SchemaBuilder.struct().optional();
                    handleNestedArray(key, nestedMap, builder, documentArrayBuilder, true);
                }
            }
            else if (docValue instanceof Map<?, ?> nestedMap) {
                String docKeyString = fieldNamer.fieldNameFor(docKey.toString());
                validateSingleTypeMap(nestedMap);
                if (nestedBuilder.field(docKeyString) == null && !nestedMap.isEmpty()) {
                    schema(docKeyString, (Entry<Object, BsonType>) nestedMap.entrySet().iterator().next(), nestedBuilder);
                }
                else {
                    for (Entry<?, ?> subEntry : nestedMap.entrySet()) {
                        schema(docKeyString, (Entry<Object, BsonType>) subEntry, nestedBuilder);
                    }
                }
            }
        }

        if (!nestedBuilder.fields().isEmpty() && builder.field(key) == null) {
            if (nested) {
                builder.field(key, SchemaBuilder.array(SchemaBuilder.array(nestedBuilder.build()).optional().build()).optional().build());
            }
            else {
                builder.field(key, SchemaBuilder.array(nestedBuilder.build()).optional().build());
            }
        }
    }

    /**
     * Handles simple arrays in the schema
     */
    private void handleSimpleArray(String key, List<?> list, SchemaBuilder builder, boolean nested) {
        if (builder.field(key) != null) {
            return;
        }

        BsonType elementType = validateSingleTypeArray(list);
        SchemaBuilder arraySchema = SchemaBuilder.array(getSchema(elementType)).optional();

        builder.field(key, nested
                ? SchemaBuilder.array(arraySchema.optional().build()).optional().build()
                : arraySchema.build());
    }

    /**
     * Validates that all elements in the array are of the same BsonType
     */
    private BsonType validateSingleTypeArray(List<?> list) {
        List<Schema> types = list.stream()
                .map(o -> getSchema(((BsonValue) o).getBsonType()))
                .distinct()
                .toList();

        if (types.size() > 1) {
            throw new DebeziumException("Array elements with different Bson types are not supported: " + list);
        }

        return ((BsonValue) list.get(0)).getBsonType();
    }

    /**
     * Validates that all elements in the map are of the same BsonType
     */
    private void validateSingleTypeMap(Map<?, ?> map) {
        List<Schema> types = map.values().stream()
                .map(o -> getSchema(((BsonType) o)))
                .distinct()
                .toList();

        if (types.size() > 1) {
            throw new DebeziumException("Array elements with different Bson types are not supported: " + map);
        }

        map.values().iterator().next();
    }

    /**
     * Returns the schema for a given BsonType
     */
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
                throw new IllegalArgumentException("The value type " + type + " is not supported for sub schema");
        }
    }

    /**
     * Builds a Struct from a BsonValue based on the provided schema
     *
     * @param bsonValueEntry the BsonValue entry to convert
     * @param schema         the schema for the Struct
     * @param struct         the Struct to populate with converted values
     */
    public void buildStruct(Entry<String, BsonValue> bsonValueEntry, Schema schema, Struct struct) {
        Object colValue = null;
        String key = fieldNamer.fieldNameFor(bsonValueEntry.getKey());
        BsonValue value = bsonValueEntry.getValue();
        BsonType type = value.getBsonType();

        switch (type) {
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
                            colValue = buildArray(value.asArray(), arraySchema, new ArrayList<>());
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
                colValue = getObject(value);
                break;

        }

        if (type != BsonType.UNDEFINED) {
            struct.put(key, colValue);
        }
    }

    /**
     * Returns the name of the array element struct based on its index.
     */
    protected String arrayElementStructName(int i) {
        return "_" + i;
    }

    /**
     * Converts a BsonValue to an Object based on its type.
     *
     * @param value the BsonValue to convert
     * @return the converted Object
     */
    private Object getObject(BsonValue value) {
        BsonType type = value.getBsonType();
        Object colValue = null;
        switch (type) {
            case STRING:
                colValue = value.asString().getValue();
                break;

            case OBJECT_ID:
                colValue = value.asObjectId().getValue().toString();
                break;

            case DECIMAL128:
                colValue = value.asDecimal128().getValue().toString();
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

            case JAVASCRIPT:
                colValue = value.asJavaScript().getCode();
                break;

            default:
                break;
        }
        return colValue;
    }

    /**
     * Builds an array of objects from a BsonArray based on the provided schema
     *
     * @param array  the BsonArray to convert
     * @param schema the schema for the array elements
     * @param values the list to populate with converted values
     * @return the populated list of objects
     */
    private Object buildArray(BsonArray array, Schema schema, ArrayList<Object> values) {
        for (BsonValue value : array) {
            BsonType type = value.getBsonType();
            switch (type) {
                case ARRAY:
                    ArrayList<Object> subValues = new ArrayList<>();
                    for (BsonValue arrayValue : value.asArray()) {
                        BsonType valueBsonType = arrayValue.getBsonType();
                        switch (valueBsonType) {
                            case ARRAY:
                                subValues.add(buildArray(arrayValue.asArray(), schema, new ArrayList<>()));
                                break;
                            case DOCUMENT:
                                BsonDocument doc = arrayValue.asDocument();
                                Struct struct = new Struct(schema.valueSchema());
                                for (Entry<String, BsonValue> entry : doc.entrySet()) {
                                    buildStruct(entry, schema.valueSchema(), struct);
                                }
                                subValues.add(struct);
                                break;
                            default:
                                subValues.add(getObject(arrayValue));
                                break;
                        }
                    }
                    if (!subValues.isEmpty()) {
                        values.add(subValues);
                    }
                    break;

                case DOCUMENT:
                    BsonDocument doc = value.asDocument();
                    Struct struct = new Struct(schema);
                    for (Entry<String, BsonValue> entry : doc.entrySet()) {
                        buildStruct(entry, schema, struct);
                    }
                    values.add(struct);
                    break;

                default:
                    values.add(getObject(value));
            }
        }
        return values;
    }
}
