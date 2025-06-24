/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Xml;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.heartbeat.DefaultHeartbeat;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.HistoryRecord;

/**
 * A factory for creating {@link SchemaBuilder} structs.
 *
 * @author Anisha Mohanty
 */

public class SchemaFactory {

    /*
     * Source info schemas
     */
    private static final int SNAPSHOT_RECORD_SCHEMA_VERSION = 1;

    public static final int SOURCE_INFO_DEFAULT_SCHEMA_VERSION = 1;

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

    protected static final String TRANSACTION_BLOCK_SCHEMA_NAME = "event.block";
    protected static final int TRANSACTION_BLOCK_SCHEMA_VERSION = 1;

    private static final String TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_NAME = "event.collection";
    private static final int TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_VERSION = 1;

    /*
     * Schema history schemas
     */
    private static final String SCHEMA_HISTORY_CONNECTOR_SCHEMA_NAME_PREFIX = "io.debezium.connector.";

    private static final String SCHEMA_HISTORY_CONNECTOR_KEY_SCHEMA_NAME_SUFFIX = ".SchemaChangeKey";
    private static final int SCHEMA_HISTORY_CONNECTOR_KEY_SCHEMA_VERSION = 1;

    public static final String SCHEMA_CHANGE_VALUE = "SchemaChangeValue";
    private static final String SCHEMA_HISTORY_CONNECTOR_VALUE_SCHEMA_NAME_SUFFIX = "." + SCHEMA_CHANGE_VALUE;
    private static final int SCHEMA_HISTORY_CONNECTOR_VALUE_SCHEMA_VERSION = 1;

    private static final String SCHEMA_HISTORY_TABLE_SCHEMA_NAME = "io.debezium.connector.schema.Table";
    private static final int SCHEMA_HISTORY_TABLE_SCHEMA_VERSION = 1;

    private static final String SCHEMA_HISTORY_COLUMN_SCHEMA_NAME = "io.debezium.connector.schema.Column";
    private static final int SCHEMA_HISTORY_COLUMN_SCHEMA_VERSION = 1;

    private static final String SCHEMA_HISTORY_CHANGE_SCHEMA_NAME = "io.debezium.connector.schema.Change";
    private static final int SCHEMA_HISTORY_CHANGE_SCHEMA_VERSION = 1;

    /*
     * Source schema block's schemas
     */
    private static final String SOURCE_SCHEMA_NAME = "io.debezium.connector.source.Schema";
    private static final Integer SOURCE_SCHEMA_VERSION = 1;
    private static final String SOURCE_SCHEMA_TABLE_SCHEMA_NAME = "io.debezium.connector.source.schema.Table";
    private static final Integer SOURCE_SCHEMA_TABLE_SCHEMA_VERSION = 1;
    private static final String SOURCE_SCHEMA_COLUMN_SCHEMA_NAME = "io.debezium.connector.source.schema.Column";
    private static final Integer SOURCE_SCHEMA_COLUMN_SCHEMA_VERSION = 1;

    /*
     * Notification schemas
     */
    private static final String NOTIFICATION_KEY_SCHEMA_NAME = "io.debezium.connector.common.NotificationKey";
    private static final Integer NOTIFICATION_KEY_SCHEMA_VERSION = 1;
    private static final String NOTIFICATION_VALUE_SCHEMA_NAME = "io.debezium.connector.common.Notification";
    private static final Integer NOTIFICATION_VALUE_SCHEMA_VERSION = 1;

    private static final SchemaFactory schemaFactoryObject = new SchemaFactory();

    public SchemaFactory() {
    }

    public static SchemaFactory get() {
        return schemaFactoryObject;
    }

    public boolean isSchemaChangeSchema(Schema schema) {
        if (schema != null && schema.name() != null) {
            return schema.name().endsWith(SCHEMA_HISTORY_CONNECTOR_VALUE_SCHEMA_NAME_SUFFIX) ||
                    schema.name().endsWith(SCHEMA_HISTORY_CONNECTOR_KEY_SCHEMA_NAME_SUFFIX);
        }
        return false;
    }

    public boolean isHeartBeatSchema(Schema schema) {
        if (schema != null && schema.name() != null) {
            return schema.name().endsWith(HEARTBEAT_VALUE_SCHEMA_NAME);
        }
        return false;
    }

    public boolean isNotificationSchema(Schema schema) {
        if (schema != null && schema.name() != null) {
            return schema.name().endsWith(NOTIFICATION_VALUE_SCHEMA_NAME);
        }
        return false;
    }

    public Schema snapshotRecordSchema() {
        return Enum.builder(
                Arrays.stream(SnapshotRecord.values()).map(java.lang.Enum::name).map(String::toLowerCase).toList())
                .version(SNAPSHOT_RECORD_SCHEMA_VERSION)
                .defaultValue(SnapshotRecord.FALSE.name().toLowerCase()).optional().build();
    }

    public SchemaBuilder sourceInfoSchemaBuilder() {
        return SchemaBuilder.struct()
                .version(SOURCE_INFO_DEFAULT_SCHEMA_VERSION)
                .field(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(AbstractSourceInfo.SNAPSHOT_KEY, snapshotRecordSchema())
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_US_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_NS_KEY, Schema.OPTIONAL_INT64_SCHEMA);
    }

    public Schema heartbeatKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(HEARTBEAT_KEY_SCHEMA_NAME))
                .version(HEARTBEAT_KEY_SCHEMA_VERSION)
                .field(DefaultHeartbeat.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
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
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema transactionEventCountPerDataCollectionSchema() {
        return SchemaBuilder.struct().optional()
                .name(TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_NAME)
                .version(TRANSACTION_EVENT_COUNT_COLLECTION_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_COLLECTION_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema transactionKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(TRANSACTION_METADATA_KEY_SCHEMA_NAME))
                .version(TRANSACTION_METADATA_KEY_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema transactionValueSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(TRANSACTION_METADATA_VALUE_SCHEMA_NAME))
                .version(TRANSACTION_METADATA_VALUE_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY,
                        SchemaBuilder.array(transactionEventCountPerDataCollectionSchema()).optional().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TS_MS, Schema.INT64_SCHEMA)
                .build();
    }

    public Schema schemaHistoryColumnSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(SCHEMA_HISTORY_COLUMN_SCHEMA_NAME))
                .version(SCHEMA_HISTORY_COLUMN_SCHEMA_VERSION)
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
                .field(ConnectTableChangeSerializer.DEFAULT_VALUE_EXPRESSION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.ENUM_VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .build();
    }

    public Schema schemaHistoryTableSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(SCHEMA_HISTORY_TABLE_SCHEMA_NAME))
                .version(SCHEMA_HISTORY_TABLE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.DEFAULT_CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.PRIMARY_KEY_COLUMN_NAMES_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field(ConnectTableChangeSerializer.COLUMNS_KEY, SchemaBuilder.array(schemaHistoryColumnSchema(adjuster)).build())
                .field(ConnectTableChangeSerializer.COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
    }

    public Schema schemaHistoryChangeSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(SCHEMA_HISTORY_CHANGE_SCHEMA_NAME))
                .version(SCHEMA_HISTORY_CHANGE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.TYPE_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.ID_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.TABLE_KEY, schemaHistoryTableSchema(adjuster))
                .build();
    }

    public Schema schemaHistoryConnectorKeySchema(SchemaNameAdjuster adjuster, CommonConnectorConfig config) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(
                        String.format("%s%s%s", SCHEMA_HISTORY_CONNECTOR_SCHEMA_NAME_PREFIX, config.getConnectorName(), SCHEMA_HISTORY_CONNECTOR_KEY_SCHEMA_NAME_SUFFIX)))
                .version(SCHEMA_HISTORY_CONNECTOR_KEY_SCHEMA_VERSION)
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema schemaHistoryConnectorValueSchema(SchemaNameAdjuster adjuster, CommonConnectorConfig config, ConnectTableChangeSerializer serializer) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(String.format("%s%s%s", SCHEMA_HISTORY_CONNECTOR_SCHEMA_NAME_PREFIX, config.getConnectorName(),
                        SCHEMA_HISTORY_CONNECTOR_VALUE_SCHEMA_NAME_SUFFIX)))
                .version(SCHEMA_HISTORY_CONNECTOR_VALUE_SCHEMA_VERSION)
                .field(HistoryRecord.Fields.SOURCE, config.getSourceInfoStructMaker().schema())
                .field(HistoryRecord.Fields.TIMESTAMP, Schema.INT64_SCHEMA)
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.TABLE_CHANGES, SchemaBuilder.array(serializer.getChangeSchema()).build())
                .build();
    }

    public Schema sourceSchemaBlockSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(SOURCE_SCHEMA_NAME))
                .version(SOURCE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.ID_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.TABLE_KEY, sourceSchemaBlockTableSchema(adjuster))
                .build();
    }

    public Schema sourceSchemaBlockTableSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(SOURCE_SCHEMA_TABLE_SCHEMA_NAME)
                .version(SOURCE_SCHEMA_TABLE_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.COLUMNS_KEY, SchemaBuilder.array(sourceSchemaBlockColumnSchema(adjuster)).build())
                .build();
    }

    public Schema sourceSchemaBlockColumnSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(SOURCE_SCHEMA_COLUMN_SCHEMA_NAME))
                .version(SOURCE_SCHEMA_COLUMN_SCHEMA_VERSION)
                .field(ConnectTableChangeSerializer.NAME_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.TYPE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.LENGTH_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.SCALE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(ConnectTableChangeSerializer.COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public Schema notificationKeySchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(NOTIFICATION_KEY_SCHEMA_NAME))
                .version(NOTIFICATION_KEY_SCHEMA_VERSION)
                .field(Notification.ID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    public Schema notificationValueSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(NOTIFICATION_VALUE_SCHEMA_NAME))
                .version(NOTIFICATION_VALUE_SCHEMA_VERSION)
                .field(Notification.ID_KEY, SchemaBuilder.STRING_SCHEMA)
                .field(Notification.TYPE, Schema.STRING_SCHEMA)
                .field(Notification.AGGREGATE_TYPE, Schema.STRING_SCHEMA)
                .field(Notification.ADDITIONAL_DATA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
                .field(Notification.TIMESTAMP, Schema.INT64_SCHEMA)
                .build();
    }

    public SchemaBuilder datatypeBitsSchema(int length) {
        return SchemaBuilder.bytes()
                .name(Bits.LOGICAL_NAME)
                .version(Bits.SCHEMA_VERSION)
                .parameter(Bits.LENGTH_FIELD, Integer.toString(length));
    }

    public SchemaBuilder datatypeEnumSchema(String allowedValues) {
        return SchemaBuilder.string()
                .name(Enum.LOGICAL_NAME)
                .version(Enum.SCHEMA_VERSION)
                .parameter(Enum.VALUES_FIELD, allowedValues);
    }

    public SchemaBuilder datatypeEnumSetSchema(String allowedValues) {
        return SchemaBuilder.string()
                .name(EnumSet.LOGICAL_NAME)
                .version(EnumSet.SCHEMA_VERSION)
                .parameter(EnumSet.VALUES_FIELD, allowedValues);
    }

    public SchemaBuilder datatypeJsonSchema() {
        return SchemaBuilder.string()
                .name(Json.LOGICAL_NAME)
                .version(Json.SCHEMA_VERSION);
    }

    public SchemaBuilder datatypeUuidSchema() {
        return SchemaBuilder.string()
                .name(Uuid.LOGICAL_NAME)
                .version(Uuid.SCHEMA_VERSION);
    }

    public SchemaBuilder datatypeVariableScaleDecimalSchema() {
        return SchemaBuilder.struct()
                .name(VariableScaleDecimal.LOGICAL_NAME)
                .version(VariableScaleDecimal.SCHEMA_VERSION)
                .doc("Variable scaled decimal")
                .field(VariableScaleDecimal.SCALE_FIELD, Schema.INT32_SCHEMA)
                .field(VariableScaleDecimal.VALUE_FIELD, Schema.BYTES_SCHEMA);
    }

    public SchemaBuilder datatypeXmlSchema() {
        return SchemaBuilder.string()
                .name(Xml.LOGICAL_NAME)
                .version(Xml.SCHEMA_VERSION);
    }

    public SchemaBuilder datatypeDoubleVectorSchema() {
        return SchemaBuilder.array(Schema.FLOAT64_SCHEMA)
                .name(DoubleVector.LOGICAL_NAME)
                .version(DoubleVector.SCHEMA_VERSION);
    }

    public SchemaBuilder datatypeFloatVectorSchema() {
        return SchemaBuilder.array(Schema.FLOAT32_SCHEMA)
                .name(FloatVector.LOGICAL_NAME)
                .version(FloatVector.SCHEMA_VERSION);
    }

    public SchemaBuilder dataTypeSparseDoubleVectorSchema() {
        return SchemaBuilder.struct()
                .name(SparseDoubleVector.LOGICAL_NAME)
                .name(SparseDoubleVector.LOGICAL_NAME)
                .version(SparseDoubleVector.SCHEMA_VERSION)
                .doc("Sparse double vector")
                .field(SparseDoubleVector.DIMENSIONS_FIELD, Schema.INT16_SCHEMA)
                .field(SparseDoubleVector.VECTOR_FIELD, SchemaBuilder.map(Schema.INT16_SCHEMA, Schema.FLOAT64_SCHEMA).build());
    }

    public Envelope.Builder datatypeEnvelopeSchema() {
        return new Envelope.Builder() {
            private final SchemaBuilder builder = SchemaBuilder.struct()
                    .version(Envelope.SCHEMA_VERSION);

            private final Set<String> missingFields = new HashSet<>();

            @Override
            public Envelope.Builder withSchema(Schema fieldSchema, String... fieldNames) {
                for (String fieldName : fieldNames) {
                    builder.field(fieldName, fieldSchema);
                }
                return this;
            }

            @Override
            public Envelope.Builder withName(String name) {
                builder.name(name);
                return this;
            }

            @Override
            public Envelope.Builder withDoc(String doc) {
                builder.doc(doc);
                return this;
            }

            @Override
            public Envelope build() {
                builder.field(Envelope.FieldName.OPERATION, Envelope.OPERATION_REQUIRED ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA);
                if (builder.field(Envelope.FieldName.TRANSACTION) == null) {
                    builder.field(Envelope.FieldName.TRANSACTION, transactionBlockSchema());
                }
                checkFieldIsDefined(Envelope.FieldName.OPERATION);
                checkFieldIsDefined(Envelope.FieldName.BEFORE);
                checkFieldIsDefined(Envelope.FieldName.AFTER);
                checkFieldIsDefined(Envelope.FieldName.SOURCE);
                checkFieldIsDefined(Envelope.FieldName.TRANSACTION);
                if (!missingFields.isEmpty()) {
                    throw new IllegalStateException("The envelope schema is missing field(s) " + String.join(", ", missingFields));
                }
                return new Envelope(builder.build());
            }

            private void checkFieldIsDefined(String fieldName) {
                if (builder.field(fieldName) == null) {
                    missingFields.add(fieldName);
                }
            }
        };
    }
}
