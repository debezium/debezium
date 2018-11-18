/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mongodb.transforms.UnwrapFromMongoDbEnvelope.ArrayEncoding;

/**
 * MongoDataConverter handles translating MongoDB strings to Kafka Connect schemas and row data to Kafka
 * Connect records.
 *
 * @author Sairam Polavarapu
 */
public class MongoDataConverter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDataConverter.class);
    public static final String SCHEMA_NAME_REGEX = "io.debezium.mongodb.regex";

    private final ArrayEncoding arrayEncoding;

    public MongoDataConverter(ArrayEncoding arrayEncoding) {
        this.arrayEncoding = arrayEncoding;
    }

    public Struct convertRecord(Entry<String, BsonValue> keyvalueforStruct, Schema schema, Struct struct) {
        convertFieldValue(keyvalueforStruct, struct, schema);
        return struct;
    }

    public void convertFieldValue(Entry<String, BsonValue> keyvalueforStruct, Struct struct, Schema schema) {
        Object colValue = null;
        String key = keyvalueforStruct.getKey();
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
            colValue = keyvalueforStruct.getValue().asDateTime().getValue();
            break;

        case JAVASCRIPT:
            colValue = keyvalueforStruct.getValue().asJavaScript().getCode();
            break;

        case JAVASCRIPT_WITH_SCOPE:
            Struct jsStruct = new Struct(schema.field(keyvalueforStruct.getKey()).schema());
            Struct jsScopeStruct = new Struct(
                    schema.field(keyvalueforStruct.getKey()).schema().field("scope").schema());
            jsStruct.put("code", keyvalueforStruct.getValue().asJavaScriptWithScope().getCode());
            BsonDocument jwsDoc = keyvalueforStruct.getValue().asJavaScriptWithScope().getScope().asDocument();

            for (Entry<String, BsonValue> jwsDocKey : jwsDoc.entrySet()) {
                convertFieldValue(jwsDocKey, jsScopeStruct, schema.field(keyvalueforStruct.getKey()).schema());
            }

            jsStruct.put("scope", jsScopeStruct);
            colValue = jsStruct;
            break;

        case REGULAR_EXPRESSION:
            Struct regexStruct = new Struct(schema.field(keyvalueforStruct.getKey()).schema());
            regexStruct.put("regex", keyvalueforStruct.getValue().asRegularExpression().getPattern());
            regexStruct.put("options", keyvalueforStruct.getValue().asRegularExpression().getOptions());
            colValue = regexStruct;
            break;

        case TIMESTAMP:
            colValue = keyvalueforStruct.getValue().asTimestamp().getTime();
            break;

        case DECIMAL128:
            colValue = keyvalueforStruct.getValue().asDecimal128().getValue().toString();
            break;

        case DOCUMENT:
            Schema documentSchema = schema.field(keyvalueforStruct.getKey()).schema();
            Struct documentStruct = new Struct(documentSchema);
            BsonDocument docs = keyvalueforStruct.getValue().asDocument();

            for (Entry<String, BsonValue> doc : docs.entrySet()) {
                convertFieldValue(doc, documentStruct, documentSchema);
            }

            colValue = documentStruct;
            break;

        case ARRAY:
            if (keyvalueforStruct.getValue().asArray().isEmpty()) {
                switch (arrayEncoding) {
                case ARRAY:
                    colValue =  new ArrayList<>();
                    break;
                case DOCUMENT:
                    final Schema fieldSchema = schema.field(keyvalueforStruct.getKey()).schema();
                    colValue = new Struct(fieldSchema);
                    break;
                }
            } else {
                switch (arrayEncoding) {
                case ARRAY:
                    BsonType valueType = keyvalueforStruct.getValue().asArray().get(0).getBsonType();
                    List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();
                    ArrayList<Object> list = new ArrayList<>();

                    arrValues.stream().forEach(arrValue -> {
                        if (arrValue.getBsonType() == BsonType.STRING && valueType == BsonType.STRING) {
                            String temp = arrValue.asString().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.JAVASCRIPT && valueType == BsonType.JAVASCRIPT) {
                            String temp = arrValue.asJavaScript().getCode();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.OBJECT_ID && valueType == BsonType.OBJECT_ID) {
                            String temp = arrValue.asObjectId().getValue().toString();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.DOUBLE && valueType == BsonType.DOUBLE) {
                            double temp = arrValue.asDouble().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.BINARY && valueType == BsonType.BINARY) {
                            byte[] temp = arrValue.asBinary().getData();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.INT32 && valueType == BsonType.INT32) {
                            int temp = arrValue.asInt32().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.INT64 && valueType == BsonType.INT64) {
                            long temp = arrValue.asInt64().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.DATE_TIME && valueType == BsonType.DATE_TIME) {
                            long temp = arrValue.asInt64().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.DECIMAL128 && valueType == BsonType.DECIMAL128) {
                            String temp = arrValue.asDecimal128().getValue().toString();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.TIMESTAMP && valueType == BsonType.TIMESTAMP) {
                            int temp = arrValue.asInt32().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.BOOLEAN && valueType == BsonType.BOOLEAN) {
                            boolean temp = arrValue.asBoolean().getValue();
                            list.add(temp);
                        } else if (arrValue.getBsonType() == BsonType.DOCUMENT && valueType == BsonType.DOCUMENT) {
                            Schema schema1 = schema.field(keyvalueforStruct.getKey()).schema().valueSchema();
                            Struct struct1 = new Struct(schema1);
                            for (Entry<String, BsonValue> entry9 : arrValue.asDocument().entrySet()) {
                                convertFieldValue(entry9, struct1, schema1);
                            }
                            list.add(struct1);
                        }
                    });
                    colValue = list;
                    break;
                case DOCUMENT:
                    final BsonArray array = keyvalueforStruct.getValue().asArray();
                    final Map<String, BsonValue> convertedArray = new HashMap<>();
                    final Schema arraySchema = schema.field(keyvalueforStruct.getKey()).schema();
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
            break;
        }
        struct.put(key, keyvalueforStruct.getValue().isNull() ? null : colValue);
    }

    protected String arrayElementStructName(int i) {
        return "_" + i;
    }

    public void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        String key = keyValuesforSchema.getKey();
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
        case TIMESTAMP:
            builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            break;

        case INT64:
        case DATE_TIME:
            builder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
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
                    BsonType valueType = keyValuesforSchema.getValue().asArray().get(0).getBsonType();
                    for (BsonValue element: keyValuesforSchema.getValue().asArray()) {
                        if (element.getBsonType() != valueType) {
                            throw new ConnectException("Field " + key + " of schema " + builder.name() + " is not a homogenous array.\n"
                                    + "Check option 'struct' of parameter 'array.encoding'");
                        }
                    }

                    switch (valueType) {
                        case NULL:
                        case STRING:
                        case JAVASCRIPT:
                        case OBJECT_ID:
                        case DECIMAL128:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
                            break;
                        case DOUBLE:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build());
                            break;
                        case BINARY:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build());
                            break;

                        case INT32:
                        case TIMESTAMP:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build());
                            break;

                        case INT64:
                        case DATE_TIME:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build());
                            break;

                        case BOOLEAN:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build());
                            break;

                        case DOCUMENT:
                            final SchemaBuilder documentSchemaBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                            final Map<String, BsonType> union = new HashMap<>();
                            for (BsonValue element: keyValuesforSchema.getValue().asArray()) {
                                final BsonDocument arrayDocs = element.asDocument();
                                for (Entry<String, BsonValue> arrayDoc: arrayDocs.entrySet()) {
                                    final BsonType prevType = union.putIfAbsent(arrayDoc.getKey(), arrayDoc.getValue().getBsonType());
                                    if (prevType == null) {
                                        addFieldSchema(arrayDoc, documentSchemaBuilder);
                                    }
                                    else if (prevType != arrayDoc.getValue().getBsonType()) {
                                        throw new ConnectException("Field " + arrayDoc.getKey() + " of schema " + documentSchemaBuilder.name() + " is not the same type for all documents in the array.\n"
                                                + "Check option 'struct' of parameter 'array.encoding'");
                                    }
                                }
                            }

                            Schema build = documentSchemaBuilder.build();
                            builder.field(key, SchemaBuilder.array(build).optional().build());
                            break;

                        default:
                            break;
                    }
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
}
