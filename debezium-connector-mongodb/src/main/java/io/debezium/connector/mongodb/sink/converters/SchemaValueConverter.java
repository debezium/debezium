/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.MAP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mongodb.sink.converters.bson.AbstractBsonType;
import io.debezium.connector.mongodb.sink.converters.bson.BooleanType;
import io.debezium.connector.mongodb.sink.converters.bson.BytesType;
import io.debezium.connector.mongodb.sink.converters.bson.DateType;
import io.debezium.connector.mongodb.sink.converters.bson.DecimalType;
import io.debezium.connector.mongodb.sink.converters.bson.Float32Type;
import io.debezium.connector.mongodb.sink.converters.bson.Float64Type;
import io.debezium.connector.mongodb.sink.converters.bson.Int16Type;
import io.debezium.connector.mongodb.sink.converters.bson.Int32Type;
import io.debezium.connector.mongodb.sink.converters.bson.Int64Type;
import io.debezium.connector.mongodb.sink.converters.bson.Int8Type;
import io.debezium.connector.mongodb.sink.converters.bson.StringType;
import io.debezium.connector.mongodb.sink.converters.bson.TimeType;
import io.debezium.connector.mongodb.sink.converters.bson.TimestampType;

/** Used for converting Struct based data with schema, like AVRO, Protobuf or JSON */
class SchemaValueConverter implements SinkValueConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValueConverter.class);
    private static final Set<String> LOGICAL_TYPE_NAMES = unmodifiableSet(
            new HashSet<>(
                    asList(
                            Date.LOGICAL_NAME,
                            Decimal.LOGICAL_NAME,
                            Time.LOGICAL_NAME,
                            Timestamp.LOGICAL_NAME)));

    private final Map<Schema.Type, AbstractBsonType> schemaTypeToBsonType = new HashMap<>();
    private final Map<String, AbstractBsonType> logicalSchemaTypeToBsonType = new HashMap<>();

    SchemaValueConverter() {
        registerType(new BooleanType());
        registerType(new BytesType());
        registerType(new Float32Type());
        registerType(new Float64Type());
        registerType(new Int8Type());
        registerType(new Int16Type());
        registerType(new Int32Type());
        registerType(new Int64Type());
        registerType(new StringType());

        registerLogicalType(new DateType());
        registerLogicalType(new DecimalType());
        registerLogicalType(new TimeType());
        registerLogicalType(new TimestampType());
    }

    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (schema == null || value == null) {
            throw new DataException("Schema-ed conversion failed due to record key and/or value was null");
        }
        return toBsonDoc(schema, value).asDocument();
    }

    private void registerType(final AbstractBsonType bsonType) {
        schemaTypeToBsonType.put(bsonType.getSchema().type(), bsonType);
    }

    private void registerLogicalType(final AbstractBsonType bsonType) {
        logicalSchemaTypeToBsonType.put(bsonType.getSchema().name(), bsonType);
    }

    private BsonValue toBsonDoc(final Schema schema, final Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        BsonDocument doc = new BsonDocument();
        if (schema.type() == MAP) {
            Schema fieldSchema = schema.valueSchema();
            Map m = (Map) value;
            for (Object entry : m.keySet()) {
                String key = (String) entry;
                if (fieldSchema.type().isPrimitive()) {
                    doc.put(key, getBsonType(fieldSchema).toBson(m.get(key), fieldSchema));
                }
                else if (fieldSchema.type().equals(ARRAY)) {
                    doc.put(key, toBsonArray(fieldSchema, m.get(key)));
                }
                else {
                    if (m.get(key) == null) {
                        doc.put(key, BsonNull.VALUE);
                    }
                    else {
                        doc.put(key, toBsonDoc(fieldSchema, m.get(key)));
                    }
                }
            }
        }
        else {
            schema.fields().forEach(f -> doc.put(f.name(), processField((Struct) value, f)));
        }
        return doc;
    }

    private BsonValue toBsonArray(final Schema schema, final Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        Schema fieldSchema = schema.valueSchema();
        BsonArray bsonArray = new BsonArray();
        List<?> myList = (List) value;
        myList.forEach(
                v -> {
                    if (fieldSchema.type().isPrimitive()) {
                        if (v == null) {
                            bsonArray.add(BsonNull.VALUE);
                        }
                        else {
                            bsonArray.add(getBsonType(fieldSchema).toBson(v));
                        }
                    }
                    else if (fieldSchema.type().equals(ARRAY)) {
                        bsonArray.add(toBsonArray(fieldSchema, v));
                    }
                    else {
                        bsonArray.add(toBsonDoc(fieldSchema, v));
                    }
                });
        return bsonArray;
    }

    private BsonValue processField(final Struct struct, final Field field) {
        LOGGER.trace("processing field '{}'", field.name());

        if (struct.get(field.name()) == null) {
            LOGGER.trace("no field in struct -> adding null");
            return BsonNull.VALUE;
        }

        if (isSupportedLogicalType(field.schema())) {
            return getBsonType(field.schema()).toBson(struct.get(field), field.schema());
        }

        try {
            switch (field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                case BYTES:
                    return handlePrimitiveField(struct, field);
                case STRUCT:
                case MAP:
                    return toBsonDoc(field.schema(), struct.get(field));
                case ARRAY:
                    return toBsonArray(field.schema(), struct.get(field));
                default:
                    throw new DataException("Unexpected / unsupported schema type " + field.schema().type());
            }
        }
        catch (Exception e) {
            throw new DataException("Error while processing field " + field.name(), e);
        }
    }

    private BsonValue handlePrimitiveField(final Struct struct, final Field field) {
        LOGGER.trace("handling primitive type '{}'", field.schema().type());
        return getBsonType(field.schema()).toBson(struct.get(field), field.schema());
    }

    private boolean isSupportedLogicalType(final Schema schema) {
        if (schema.name() == null) {
            return false;
        }
        return LOGICAL_TYPE_NAMES.contains(schema.name());
    }

    private AbstractBsonType getBsonType(final Schema schema) {
        AbstractBsonType bsonType;

        if (isSupportedLogicalType(schema)) {
            bsonType = logicalSchemaTypeToBsonType.get(schema.name());
        }
        else {
            bsonType = schemaTypeToBsonType.get(schema.type());
        }

        if (bsonType == null) {
            throw new ConnectException(
                    "error no registered bsonType found for " + schema.type().getName());
        }
        return bsonType;
    }
}
