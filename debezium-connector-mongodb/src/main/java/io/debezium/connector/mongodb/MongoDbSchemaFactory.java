/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.Json;
import io.debezium.schema.SchemaFactory;

public class MongoDbSchemaFactory extends SchemaFactory {

    public MongoDbSchemaFactory() {
        super();
    }

    private static final MongoDbSchemaFactory mongoDbSchemaFactoryObject = new MongoDbSchemaFactory();

    public static MongoDbSchemaFactory get() {
        return mongoDbSchemaFactoryObject;
    }

    /*
     * MongoDb Change Stream schema
     */
    private static final int MONGODB_TRUNCATED_ARRAY_SCHEMA_VERSION = 1;
    private static final int MONGODB_UPDATED_DESCRIPTION_SCHEMA_VERSION = 1;
    private static final int MONGODB_BSON_TIMESTAMP_SCHEMA_VERSION = 1;

    public Schema truncatedArraySchema() {
        return SchemaBuilder.struct()
                .name(MongoDbSchema.SCHEMA_NAME_TRUNCATED_ARRAY)
                .version(MONGODB_TRUNCATED_ARRAY_SCHEMA_VERSION)
                .field(MongoDbFieldName.ARRAY_FIELD_NAME, Schema.STRING_SCHEMA)
                .field(MongoDbFieldName.ARRAY_NEW_SIZE, Schema.INT32_SCHEMA)
                .build();
    }

    public Schema updatedDescriptionSchema() {
        return SchemaBuilder.struct()
                .optional()
                .name(MongoDbSchema.SCHEMA_NAME_UPDATED_DESCRIPTION)
                .version(MONGODB_UPDATED_DESCRIPTION_SCHEMA_VERSION)
                .field(MongoDbFieldName.REMOVED_FIELDS,
                        SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field(MongoDbFieldName.UPDATED_FIELDS,
                        Json.builder().optional().build())
                .field(MongoDbFieldName.TRUNCATED_ARRAYS,
                        SchemaBuilder.array(MongoDbSchema.TRUNCATED_ARRAY_SCHEMA).optional().build())
                .build();
    }

    public Schema bsonTimestampSchema() {
        // A BSON Timestamp always carries both components, so neither subfield is optional. Both are
        // unsigned 32-bit values in BSON; they are widened to (signed) INT64 because Connect has no
        // unsigned types and the full unsigned range must stay exact.
        return SchemaBuilder.struct()
                .name(MongoDbSchema.SCHEMA_NAME_TIMESTAMP)
                .version(MONGODB_BSON_TIMESTAMP_SCHEMA_VERSION)
                .optional()
                .field("time", Schema.INT64_SCHEMA)
                .field("increment", Schema.INT64_SCHEMA)
                .build();
    }
}
