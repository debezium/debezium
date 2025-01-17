/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.data.Ltree;
import io.debezium.data.Envelope;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class PostgresSchemaFactory extends SchemaFactory {

    public PostgresSchemaFactory() {
        super();
    }

    private static final PostgresSchemaFactory postgresSchemaFactoryObject = new PostgresSchemaFactory();

    public static PostgresSchemaFactory get() {
        return postgresSchemaFactoryObject;
    }

    /*
     * Postgres LogicalDecodingMessageMonitor schema
     */
    public static final String POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_NAME = "io.debezium.connector.postgresql.MessageValue";
    private static final int POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_VERSION = 1;

    private static final String POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_KEY_SCHEMA_NAME = "io.debezium.connector.postgresql.MessageKey";
    private static final int POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_KEY_SCHEMA_VERSION = 1;

    private static final String POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_BLOCK_SCHEMA_NAME = "io.debezium.connector.postgresql.Message";
    private static final int POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_BLOCK_SCHEMA_VERSION = 1;

    public Schema logicalDecodingMessageMonitorKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_KEY_SCHEMA_NAME))
                .version(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_KEY_SCHEMA_VERSION)
                .field(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public Schema logicalDecodingMessageMonitorBlockSchema(SchemaNameAdjuster adjuster, CommonConnectorConfig.BinaryHandlingMode binaryHandlingMode) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_BLOCK_SCHEMA_NAME))
                .version(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_BLOCK_SCHEMA_VERSION)
                .field(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY, binaryHandlingMode.getSchema().optional().build())
                .build();
    }

    public Schema logicalDecodingMessageMonitorValueSchema(SchemaNameAdjuster adjuster, PostgresConnectorConfig config,
                                                           CommonConnectorConfig.BinaryHandlingMode binaryHandlingMode) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_NAME))
                .version(POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_VERSION)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.SOURCE, config.getSourceInfoStructMaker().schema())
                .field(Envelope.FieldName.TRANSACTION, config.getTransactionMetadataFactory().getTransactionStructMaker().getTransactionBlockSchema())
                .field(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY, logicalDecodingMessageMonitorBlockSchema(adjuster, binaryHandlingMode))
                .build();
    }

    public SchemaBuilder datatypeLtreeSchema() {
        return SchemaBuilder.string()
                .name(Ltree.LOGICAL_NAME)
                .version(Ltree.SCHEMA_VERSION);
    }
}
