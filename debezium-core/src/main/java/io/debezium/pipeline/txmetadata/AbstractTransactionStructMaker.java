/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static io.debezium.config.CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public abstract class AbstractTransactionStructMaker implements TransactionStructMaker {

    protected static final Schema EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA = SchemaFactory.get().transactionEventCountPerDataCollectionSchema();
    protected Schema transactionKeySchema;
    protected Schema transactionValueSchema;

    public AbstractTransactionStructMaker(Configuration config) {
        SchemaNameAdjuster adjuster = CommonConnectorConfig.SchemaNameAdjustmentMode.parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE)).createAdjuster();
        transactionKeySchema = SchemaFactory.get().transactionKeySchema(adjuster);
        transactionValueSchema = SchemaFactory.get().transactionValueSchema(adjuster);
    }

    @Override
    public Struct addTransactionBlock(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value) {
        TransactionContext transactionContext = offsetContext.getTransactionContext();
        final Struct txStruct = new Struct(getTransactionBlockSchema());
        txStruct.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId());
        txStruct.put(DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, transactionContext.getTotalEventCount());
        txStruct.put(DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, dataCollectionEventOrder);
        return txStruct;
    }

    @Override
    public Struct buildEndTransactionValue(OffsetContext offsetContext, Instant timestamp) {
        TransactionContext transactionContext = offsetContext.getTransactionContext();
        final Struct value = new Struct(getTransactionValueSchema());
        value.put(DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.END.name());
        value.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId());
        value.put(DEBEZIUM_TRANSACTION_TS_MS, timestamp.toEpochMilli());
        value.put(DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, transactionContext.getTotalEventCount());

        final Set<Map.Entry<String, Long>> perTableEventCount = transactionContext.getPerTableEventCount().entrySet();
        final List<Struct> valuePerTableCount = new ArrayList<>(perTableEventCount.size());
        for (Map.Entry<String, Long> tableEventCount : perTableEventCount) {
            final Struct perTable = new Struct(getEventCountPerDataCollectionSchema());
            perTable.put(DEBEZIUM_TRANSACTION_COLLECTION_KEY, tableEventCount.getKey());
            perTable.put(DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, tableEventCount.getValue());
            valuePerTableCount.add(perTable);
        }
        value.put(DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY, valuePerTableCount);

        return value;
    }

    @Override
    public Struct buildBeginTransactionValue(OffsetContext offsetContext, Instant timestamp) {
        TransactionContext transactionContext = offsetContext.getTransactionContext();
        final Struct value = new Struct(getTransactionValueSchema());
        value.put(DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.BEGIN.name());
        value.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId());
        value.put(DEBEZIUM_TRANSACTION_TS_MS, timestamp.toEpochMilli());
        return value;
    }

    @Override
    public Struct buildTransactionKey(OffsetContext offsetContext) {
        TransactionContext transactionContext = offsetContext.getTransactionContext();
        final Struct key = new Struct(getTransactionKeySchema());
        key.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId());
        return key;
    }

    @Override
    public Schema getEventCountPerDataCollectionSchema() {
        return EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA;
    }

    @Override
    public Schema getTransactionKeySchema() {
        return transactionKeySchema;
    }

    @Override
    public Schema getTransactionValueSchema() {
        return transactionValueSchema;
    }
}
