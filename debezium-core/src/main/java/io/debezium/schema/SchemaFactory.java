/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;

/**
 * A factory for creating {@link SchemaBuilder} structs.
 *
 * @author Anisha Mohanty
 */

public class SchemaFactory {

    /*
     * Heartbeat schemas
     */
    private static final String HEARTBEAT_KEY_SCHEMA_NAME = "io.debezium.connector.common.ServerNameKey";
    private static final int HEARTBEAT_KEY_SCHEMA_VERSION = 1;

    private static final String HEARTBEAT_VALUE_SCHEMA_NAME = "io.debezium.connector.common.Heartbeat";
    private static final int HEARTBEAT_VALUE_SCHEMA_VERSION = 1;

    /*
     * Transaction-related schemas
     */
    private static final String TRANSACTION_METADATA_KEY_SCHEMA_NAME = "io.debezium.connector.common.TransactionMetadataKey";
    private static final int TRANSACTION_METADATA_KEY_SCHEMA_VERSION = 1;

    private static final String TRANSACTION_METADATA_VALUE_SCHEMA_NAME = "io.debezium.connector.common.TransactionMetadataValue";
    private static final int TRANSACTION_METADATA_VALUE_SCHEMA_VERSION = 1;

    private static final String TRANSACTION_BLOCK_SCHEMA_NAME = "event.block";
    private static final int TRANSACTION_BLOCK_SCHEMA_VERSION = 1;

    private static final String TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_NAME = "event.collection";
    private static final int TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_VERSION = 1;

    /*
     * Connect Table schemas
     */
    private static final String CONNECT_TABLE_CHANGE_SERIALIZER_COLUMN_SCHEMA_NAME = "io.debezium.connector.schema.Column";
    private static final int CONNECT_TABLE_CHANGE_SERIALIZER_COLUMN_SCHEMA_VERSION = 1;

    private static final String CONNECT_TABLE_CHANGE_SERIALIZER_TABLE_SCHEMA_NAME = "io.debezium.connector.schema.Table";
    private static final int CONNECT_TABLE_CHANGE_SERIALIZER_TABLE_SCHEMA_VERSION = 1;

    private static final String CONNECT_TABLE_CHANGE_SERIALIZER_CHANGE_SCHEMA_NAME = "io.debezium.connector.schema.Change";
    private static final int CONNECT_TABLE_CHANGE_SERIALIZER_CHANGE_SCHEMA_VERSION = 1;

    /*
     * Event Dispatcher schemas
     */
    private static final String EVENT_DISPATCHER_SCHEMA_NAME_PREFIX = "io.debezium.connector.";

    private static final String EVENT_DISPATCHER_KEY_SCHEMA_NAME_SUFFIX = ".SchemaChangeKey";
    private static final int EVENT_DISPATCHER_KEY_SCHEMA_VERSION = 1;

    private static final String EVENT_DISPATCHER_VALUE_SCHEMA_NAME_SUFFIX = ".SchemaChangeValue";
    private static final int EVENT_DISPATCHER_VALUE_SCHEMA_VERSION = 1;

    private static final SchemaFactory schemaFactoryObject = new SchemaFactory();

    private SchemaFactory() {
    }

    public static SchemaFactory get() {
        return schemaFactoryObject;
    }

    public Schema heartbeatKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(HEARTBEAT_KEY_SCHEMA_NAME))
                .version(HEARTBEAT_KEY_SCHEMA_VERSION)
                .field(HeartbeatImpl.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema heartbeatValueSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(HEARTBEAT_VALUE_SCHEMA_NAME))
                .version(HEARTBEAT_VALUE_SCHEMA_VERSION)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema transactionBlockSchema() {
        return SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema transactionEventCountPerDataCollectionSchema() {
        return SchemaBuilder.struct().optional()
                .name(TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_NAME)
                .version(TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_VERSION)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_COLLECTION_KEY, Schema.STRING_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema transactionKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(TRANSACTION_METADATA_KEY_SCHEMA_NAME))
                .version(TRANSACTION_METADATA_KEY_SCHEMA_VERSION)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema transactionValueSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(TRANSACTION_METADATA_VALUE_SCHEMA_NAME))
                .version(TRANSACTION_METADATA_VALUE_SCHEMA_VERSION)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_STATUS_KEY, Schema.STRING_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY, SchemaBuilder.array(transactionEventCountPerDataCollectionSchema()))
                .field(TransactionMonitor.DEBEZIUM_TRANSACTION_TS_MS, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema connectTableChangeSerializerColumnSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(CONNECT_TABLE_CHANGE_SERIALIZER_COLUMN_SCHEMA_NAME))
                .version(CONNECT_TABLE_CHANGE_SERIALIZER_COLUMN_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.NAME_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.JDBC_TYPE_KEY, Schema.INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.NATIVE_TYPE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.TYPE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.TYPE_EXPRESSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.LENGTH_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.SCALE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.POSITION_KEY, Schema.INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.OPTIONAL_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field(ConnectTableChangeSerializer.AUTO_INCREMENTED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field(ConnectTableChangeSerializer.GENERATED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field(ConnectTableChangeSerializer.COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public Schema connectTableChangeSerializerTableSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(CONNECT_TABLE_CHANGE_SERIALIZER_TABLE_SCHEMA_NAME))
                .version(CONNECT_TABLE_CHANGE_SERIALIZER_TABLE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.DEFAULT_CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.PRIMARY_KEY_COLUMN_NAMES_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field(ConnectTableChangeSerializer.COLUMNS_KEY, SchemaBuilder.array(connectTableChangeSerializerColumnSchema(adjuster)).build())
                .field(ConnectTableChangeSerializer.COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public Schema connectTableChangeSerializerChangeSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(CONNECT_TABLE_CHANGE_SERIALIZER_CHANGE_SCHEMA_NAME))
                .version(CONNECT_TABLE_CHANGE_SERIALIZER_CHANGE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.TYPE_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.ID_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.TABLE_KEY, connectTableChangeSerializerTableSchema(adjuster))
                .build();
    }

    public Schema eventDispatcherKeySchema(SchemaNameAdjuster adjuster, CommonConnectorConfig config) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(String.format("%s%s%s", EVENT_DISPATCHER_SCHEMA_NAME_PREFIX, config.getConnectorName(), EVENT_DISPATCHER_KEY_SCHEMA_NAME_SUFFIX)))
                .version(EVENT_DISPATCHER_KEY_SCHEMA_VERSION)
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema eventDispatcherValueSchema(SchemaNameAdjuster adjuster, CommonConnectorConfig config, ConnectTableChangeSerializer serializer) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(String.format("%s%s%s", EVENT_DISPATCHER_SCHEMA_NAME_PREFIX, config.getConnectorName(), EVENT_DISPATCHER_VALUE_SCHEMA_NAME_SUFFIX)))
                .version(EVENT_DISPATCHER_VALUE_SCHEMA_VERSION)
                .field(HistoryRecord.Fields.SOURCE, config.getSourceInfoStructMaker().schema())
                .field(HistoryRecord.Fields.TIMESTAMP, Schema.INT64_SCHEMA)
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.TABLE_CHANGES, SchemaBuilder.array(serializer.getChangeSchema()).build())
                .build();
    }
}
