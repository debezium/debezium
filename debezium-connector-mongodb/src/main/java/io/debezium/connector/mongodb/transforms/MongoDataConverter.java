/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConverter.class);
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

    public TreeMap<String, Map<Object, BsonType>> parse(BsonDocument document) {
        if (document.isEmpty()) {
            return new TreeMap<>();
        }

        TreeMap<String, Map<Object, BsonType>> map = new TreeMap<>();

        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String key = entry.getKey();
            BsonValue value = entry.getValue();
            BsonType type = value.getBsonType();

            switch (type) {
                case ARRAY:
                    map.put(key, Map.of(traversal(key, value.asArray()), type));
                    break;
                case DOCUMENT:
                    map.put(key, Map.of(parse(value.asDocument()), type));
                    break;
                default:
                    map.put(key, Map.of(value, type));
                    break;
            }
        }
        return map;
    }

    public TreeMap<String, Map<Object, BsonType>> traversal(String key, BsonArray document) {
        TreeMap<String, Map<Object, BsonType>> map = new TreeMap<>();

        for (BsonValue value : document) {
            if (value.getBsonType() == BsonType.ARRAY) {
                map.put("", Map.of(traversal(key, value.asArray()), value.getBsonType()));
            }
            else if (value.getBsonType() == BsonType.DOCUMENT) {
                map.putAll(parse(value.asDocument()));
            }
            else {
                map.put("", Map.of(value, value.getBsonType()));
            }
        }

        return map;
    }

    public SchemaBuilder buildSchema(TreeMap<String, Map<Object, BsonType>> map) {
        SchemaBuilder builder = SchemaBuilder.struct();
        for (Map.Entry<String, Map<Object, BsonType>> entry : map.entrySet()) {
            String key = fieldNamer.fieldNameFor(entry.getKey());
            schema(key, entry.getValue(), builder);
        }
        return builder;
    }

    public SchemaBuilder schema(String key, Map<Object, BsonType> map, SchemaBuilder builder) {
        for (Map.Entry<Object, BsonType> entry : map.entrySet()) {
            Object value = entry.getKey();
            BsonType type = entry.getValue();
            switch (type) {
                case ARRAY:
                    if (value == null) {
                        builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional());
                    }
                    else if (value instanceof Map<?, ?>) {
                        SchemaBuilder arrayBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                        for (Map.Entry<?, ?> doc : ((Map<?, ?>) value).entrySet()) {
                            String k = doc.getKey().toString();
                            Object v = doc.getValue();
                            if (v instanceof Map<?, ?>) {
                                schema(k, (Map<Object, BsonType>) v, arrayBuilder);
                            }
                            else {
                                arrayBuilder.field(k, getType(((BsonValue) v).getBsonType()));
                            }
                        }
                        builder.field(key, SchemaBuilder.array(arrayBuilder).optional());
                    }
                    break;
                case DOCUMENT:
                    if (value == null) {
                        builder.field(key, SchemaBuilder.struct().optional().build());
                    }
                    else {
                        if (value instanceof Map<?, ?>) {
                            SchemaBuilder documentBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                            for (Map.Entry<?, ?> doc : ((Map<?, ?>) value).entrySet()) {
                                String k = doc.getKey().toString();
                                Object v = doc.getValue();
                                if (v instanceof Map<?, ?>) {
                                    schema(k, (Map<Object, BsonType>) v, documentBuilder);
                                }
                                else {
                                    documentBuilder.field(k, getType(((BsonValue) v).getBsonType()));
                                }
                            }
                            builder.field(key, documentBuilder);
                        }
                        else {
                            builder.field(key, getType(type));
                        }
                    }
                    break;
                default:
                    if (key.isEmpty()) {
                        BsonType t = ((BsonValue) value).getBsonType();
                        return SchemaBuilder.array(getType(t)).optional();
                    }
                    else {
                        builder.field(key, getType(type));
                    }
                    break;
            }
        }
        return builder;
    }

    public Schema getType(BsonType type) {
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
                return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        }
        return null;
    }

    public Struct buildStruct(BsonDocument document, Schema schema, Struct struct) {
        Object colValue = null;
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String key = fieldNamer.fieldNameFor(entry.getKey());
            BsonValue value = entry.getValue();
            BsonType type = value.getBsonType();

            switch (type) {
                case NULL:
                    colValue = null;
                    break;

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

                    buildStruct(jwsDoc, schema.field(key).schema(), jsScopeStruct);

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
                            return struct;
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
                                colValue = buildArray(value.asArray(), schema.field(key).schema().valueSchema());
                                break;
                            case DOCUMENT:
                                // to-do
                        }
                    }
                    break;

                case DOCUMENT:
                    Field field = schema.field(key);
                    if (field == null) {
                        LOGGER.warn("Can't find field: {} in schema {}", key, schema.fields());
                        return struct;
                    }
                    Schema documentSchema = field.schema();
                    Struct documentStruct = new Struct(documentSchema);
                    BsonDocument doc = value.asDocument();
                    buildStruct(doc, documentSchema, documentStruct);
                    colValue = documentStruct;
                    break;

                default:
                    break;
            }
            struct.put(key, value.isNull() ? null : colValue);
        }
        return struct;
    }

    private Object buildArray(BsonArray array, Schema schema) {
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            BsonValue value = array.get(i);
            BsonType type = value.getBsonType();
            switch (type) {
                case ARRAY:
                    values.add(buildArray(value.asArray(), schema.valueSchema()));
                    break;
                case DOCUMENT:
                    values.add(buildStruct(value.asDocument(), schema, new Struct(schema)));
                    break;
                default:
                    values.add(value.isNull() ? null : value.asString().getValue());
                    break;
            }
        }
        return values;
    }
}
