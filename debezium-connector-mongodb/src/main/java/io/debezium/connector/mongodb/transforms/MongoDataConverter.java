/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * MongoDataConverter handles translating MongoDB strings to Kafka Connect schemas and row data to Kafka
 * Connect records.
 *
 * @author Sairam Polavarapu
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

    public Struct convertRecord(Entry<String, BsonValue> keyvalueforStruct, Schema schema, Struct struct) {
        convertFieldValue(keyvalueforStruct, struct, schema);
        return struct;
    }

    public void convertFieldValue(Entry<String, BsonValue> keyvalueforStruct, Struct struct, Schema schema) {
        Object colValue = null;
        String key = fieldNamer.fieldNameFor(keyvalueforStruct.getKey());
        BsonType type = keyvalueforStruct.getValue().getBsonType();

        switch (type) {

            case NULL:
                colValue = null;
                break;

            case STRING:
                colValue = keyvalueforStruct.getValue().asString().getValue().toString();
                break;

            case OBJECT_ID:
                colValue = keyvalueforStruct.getValue().asObjectId().getValue().toString();
                break;

            case DOUBLE:
                colValue = keyvalueforStruct.getValue().asDouble().getValue();
                break;

            case BINARY:
                colValue = keyvalueforStruct.getValue().asBinary().getData();
                break;

            case INT32:
                colValue = keyvalueforStruct.getValue().asInt32().getValue();
                break;

            case INT64:
                colValue = keyvalueforStruct.getValue().asInt64().getValue();
                break;

            case BOOLEAN:
                colValue = keyvalueforStruct.getValue().asBoolean().getValue();
                break;

            case DATE_TIME:
                colValue = new Date(keyvalueforStruct.getValue().asDateTime().getValue());
                break;

            case JAVASCRIPT:
                colValue = keyvalueforStruct.getValue().asJavaScript().getCode();
                break;

            case JAVASCRIPT_WITH_SCOPE:
                Struct jsStruct = new Struct(schema.field(key).schema());
                Struct jsScopeStruct = new Struct(
                        schema.field(key).schema().field("scope").schema());
                jsStruct.put("code", keyvalueforStruct.getValue().asJavaScriptWithScope().getCode());
                BsonDocument jwsDoc = keyvalueforStruct.getValue().asJavaScriptWithScope().getScope().asDocument();

                for (Entry<String, BsonValue> jwsDocKey : jwsDoc.entrySet()) {
                    convertFieldValue(jwsDocKey, jsScopeStruct, schema.field(key).schema());
                }

                jsStruct.put("scope", jsScopeStruct);
                colValue = jsStruct;
                break;

            case REGULAR_EXPRESSION:
                Struct regexStruct = new Struct(schema.field(key).schema());
                regexStruct.put("regex", keyvalueforStruct.getValue().asRegularExpression().getPattern());
                regexStruct.put("options", keyvalueforStruct.getValue().asRegularExpression().getOptions());
                colValue = regexStruct;
                break;

            case TIMESTAMP:
                colValue = new Date(1000L * keyvalueforStruct.getValue().asTimestamp().getTime());
                break;

            case DECIMAL128:
                colValue = keyvalueforStruct.getValue().asDecimal128().getValue().toString();
                break;

            case DOCUMENT:
                Field field = schema.field(key);
                if (field == null) {
                    throw new DataException("Failed to find field '" + key + "' in schema " + schema.name());
                }
                Schema documentSchema = field.schema();
                Struct documentStruct = new Struct(documentSchema);
                BsonDocument docs = keyvalueforStruct.getValue().asDocument();

                for (Entry<String, BsonValue> doc : docs.entrySet()) {
                    convertFieldValue(doc, documentStruct, documentSchema);
                }

                colValue = documentStruct;
                break;

            case ARRAY:
                if (keyvalueforStruct.getValue().asArray().isEmpty()) {
                    // export empty arrays as null for Avro
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
                            BsonType valueType = keyvalueforStruct.getValue().asArray().get(0).getBsonType();
                            List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();
                            ArrayList<Object> list = new ArrayList<>();

                            arrValues.stream().forEach(arrValue -> {
                                final Schema valueSchema;
                                if (Arrays.asList(BsonType.ARRAY, BsonType.DOCUMENT).contains(valueType)) {
                                    valueSchema = schema.field(key).schema().valueSchema();
                                }
                                else {
                                    valueSchema = null;
                                }
                                convertFieldValue(valueSchema, valueType, arrValue, list);
                            });
                            colValue = list;
                            break;
                        case DOCUMENT:
                            final BsonArray array = keyvalueforStruct.getValue().asArray();
                            final Map<String, BsonValue> convertedArray = new HashMap<>();
                            final Schema arraySchema = schema.field(key).schema();
                            final Struct arrayStruct = new Struct(arraySchema);
                            for (int i = 0; i < array.size(); i++) {
                                convertedArray.put(arrayElementStructName(i), array.get(i));
                            }
                            convertedArray.entrySet().forEach(x -> {
                                final Schema elementSchema = schema.field(key).schema();
                                convertFieldValue(x, arrayStruct, elementSchema);
                            });
                            colValue = arrayStruct;
                            break;
                    }
                }
                break;

            default:
                return;
        }
        struct.put(key, keyvalueforStruct.getValue().isNull() ? null : colValue);
    }

    private void convertFieldValue(Schema valueSchema, BsonType valueType, BsonValue arrValue, ArrayList<Object> list) {
        if (arrValue.getBsonType() == BsonType.STRING && valueType == BsonType.STRING) {
            String temp = arrValue.asString().getValue();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.JAVASCRIPT && valueType == BsonType.JAVASCRIPT) {
            String temp = arrValue.asJavaScript().getCode();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.OBJECT_ID && valueType == BsonType.OBJECT_ID) {
            String temp = arrValue.asObjectId().getValue().toString();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.DOUBLE && valueType == BsonType.DOUBLE) {
            double temp = arrValue.asDouble().getValue();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.BINARY && valueType == BsonType.BINARY) {
            byte[] temp = arrValue.asBinary().getData();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.INT32 && valueType == BsonType.INT32) {
            int temp = arrValue.asInt32().getValue();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.INT64 && valueType == BsonType.INT64) {
            long temp = arrValue.asInt64().getValue();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.DATE_TIME && valueType == BsonType.DATE_TIME) {
            Date temp = new Date(arrValue.asInt64().getValue());
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.DECIMAL128 && valueType == BsonType.DECIMAL128) {
            String temp = arrValue.asDecimal128().getValue().toString();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.TIMESTAMP && valueType == BsonType.TIMESTAMP) {
            Date temp = new Date(1000L * arrValue.asInt32().getValue());
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.BOOLEAN && valueType == BsonType.BOOLEAN) {
            boolean temp = arrValue.asBoolean().getValue();
            list.add(temp);
        }
        else if (arrValue.getBsonType() == BsonType.DOCUMENT && valueType == BsonType.DOCUMENT) {
            Struct struct1 = new Struct(valueSchema);
            for (Entry<String, BsonValue> entry9 : arrValue.asDocument().entrySet()) {
                convertFieldValue(entry9, struct1, valueSchema);
            }
            list.add(struct1);
        }
        else if (arrValue.getBsonType() == BsonType.ARRAY && valueType == BsonType.ARRAY) {
            ArrayList<Object> subList = new ArrayList<>();
            final Schema subValueSchema;
            if (Arrays.asList(BsonType.ARRAY, BsonType.DOCUMENT).contains(arrValue.asArray().get(0).getBsonType())) {
                subValueSchema = valueSchema.valueSchema();
            }
            else {
                subValueSchema = null;
            }
            for (BsonValue v : arrValue.asArray()) {
                convertFieldValue(subValueSchema, v.getBsonType(), v, subList);
            }
            list.add(subList);
        }
    }

    protected String arrayElementStructName(int i) {
        return "_" + i;
    }

    public void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        String key = fieldNamer.fieldNameFor(keyValuesforSchema.getKey());
        BsonType type = keyValuesforSchema.getValue().getBsonType();

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
                builder.field(key, org.apache.kafka.connect.data.Timestamp.builder().optional().build());
                break;

            case BOOLEAN:
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                break;

            case JAVASCRIPT_WITH_SCOPE:
                SchemaBuilder jswithscope = SchemaBuilder.struct().name(builder.name() + "." + key);
                jswithscope.field("code", Schema.OPTIONAL_STRING_SCHEMA);
                SchemaBuilder scope = SchemaBuilder.struct().name(jswithscope.name() + ".scope").optional();
                BsonDocument jwsDocument = keyValuesforSchema.getValue().asJavaScriptWithScope().getScope().asDocument();

                for (Entry<String, BsonValue> jwsDocumentKey : jwsDocument.entrySet()) {
                    addFieldSchema(jwsDocumentKey, scope);
                }

                Schema scopeBuild = scope.build();
                jswithscope.field("scope", scopeBuild).build();
                builder.field(key, jswithscope);
                break;

            case REGULAR_EXPRESSION:
                SchemaBuilder regexwop = SchemaBuilder.struct().name(SCHEMA_NAME_REGEX).optional();
                regexwop.field("regex", Schema.OPTIONAL_STRING_SCHEMA);
                regexwop.field("options", Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(key, regexwop.build());
                break;

            case DOCUMENT:
                SchemaBuilder builderDoc = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                BsonDocument docs = keyValuesforSchema.getValue().asDocument();

                for (Entry<String, BsonValue> doc : docs.entrySet()) {
                    addFieldSchema(doc, builderDoc);
                }
                builder.field(key, builderDoc.build());
                break;

            case ARRAY:
                if (keyValuesforSchema.getValue().asArray().isEmpty()) {
                    // ignore empty arrays; currently only for Avro, but might be worth doing in general as we
                    // cannot conclude an element type in any meaningful way
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
                            BsonArray value = keyValuesforSchema.getValue().asArray();
                            BsonType valueType = value.get(0).getBsonType();
                            testType(builder, key, keyValuesforSchema.getValue(), valueType);
                            builder.field(key, SchemaBuilder.array(subSchema(builder, key, valueType, value)).optional().build());
                            break;
                        case DOCUMENT:
                            final BsonArray array = keyValuesforSchema.getValue().asArray();
                            final SchemaBuilder arrayStructBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                            final Map<String, BsonValue> convertedArray = new HashMap<>();
                            for (int i = 0; i < array.size(); i++) {
                                convertedArray.put(arrayElementStructName(i), array.get(i));
                            }
                            convertedArray.entrySet().forEach(x -> addFieldSchema(x, arrayStructBuilder));
                            builder.field(key, arrayStructBuilder.build());
                            break;
                    }
                }
                break;
            default:
                break;
        }
    }

    private Schema subSchema(SchemaBuilder builder, String key, BsonType valueType, BsonValue value) {
        switch (valueType) {
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
                return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case DOCUMENT:
                final SchemaBuilder documentSchemaBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                final Map<String, BsonType> union = new HashMap<>();
                if (value.isArray()) {
                    value.asArray().forEach(f -> subSchema(documentSchemaBuilder, union, f.asDocument(), true));
                    if (documentSchemaBuilder.fields().size() == 0) {
                        value.asArray().forEach(f -> subSchema(documentSchemaBuilder, union, f.asDocument(), false));
                    }
                }
                else {
                    subSchema(documentSchemaBuilder, union, value.asDocument(), false);
                }
                return documentSchemaBuilder.build();
            case ARRAY:
                BsonType subValueType = value.asArray().get(0).asArray().get(0).getBsonType();
                return SchemaBuilder.array(subSchema(builder, key, subValueType, value.asArray().get(0))).optional().build();
            default:
                throw new IllegalArgumentException("The value type '" + valueType + " is not yet supported inside for a subSchema.");
        }
    }

    private void subSchema(SchemaBuilder documentSchemaBuilder, Map<String, BsonType> union, BsonDocument arrayDocs, boolean emptyChecker) {
        for (Entry<String, BsonValue> arrayDoc : arrayDocs.entrySet()) {
            final String key = fieldNamer.fieldNameFor(arrayDoc.getKey());
            if (emptyChecker && ((arrayDoc.getValue() instanceof BsonDocument && ((BsonDocument) arrayDoc.getValue()).size() == 0)
                    || (arrayDoc.getValue() instanceof BsonArray && ((BsonArray) arrayDoc.getValue()).size() == 0))) {
                continue;
            }
            final BsonType prevType = union.putIfAbsent(key, arrayDoc.getValue().getBsonType());
            if (prevType == null) {
                addFieldSchema(arrayDoc, documentSchemaBuilder);
            }
            else if (prevType != arrayDoc.getValue().getBsonType()) {
                throw new ConnectException("Field " + key + " of schema " + documentSchemaBuilder.name()
                        + " is not the same type for all documents in the array.\n"
                        + "Check option 'struct' of parameter 'array.encoding'");
            }
        }
    }

    private void testType(SchemaBuilder builder, String key, BsonValue value, BsonType valueType) {
        if (valueType == BsonType.DOCUMENT) {
            final Map<String, BsonType> union = new HashMap<>();
            for (BsonValue element : value.asArray()) {
                final BsonDocument arrayDocs = element.asDocument();
                for (Entry<String, BsonValue> arrayDoc : arrayDocs.entrySet()) {
                    final String docKey = fieldNamer.fieldNameFor(arrayDoc.getKey());
                    final BsonType prevType = union.putIfAbsent(docKey, arrayDoc.getValue().getBsonType());
                    if (prevType != null && prevType != arrayDoc.getValue().getBsonType()) {
                        throw new ConnectException("Field " + docKey + " of schema " + builder.name()
                                + " is not the same type for all documents in the array.\n"
                                + "Check option 'struct' of parameter 'array.encoding'");
                    }
                }
            }
        }
        else if (valueType == BsonType.ARRAY) {
            for (BsonValue element : value.asArray()) {
                BsonType subValueType = element.asArray().get(0).getBsonType();
                testType(builder, key, element, subValueType);
            }
        }
        else {
            for (BsonValue element : value.asArray()) {
                if (element.getBsonType() != valueType) {
                    throw new ConnectException("Field " + key + " of schema " + builder.name() + " is not a homogenous array.\n"
                            + "Check option 'struct' of parameter 'array.encoding'");
                }
            }
        }
    }
}
