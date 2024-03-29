/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.SchemaNameAdjuster;

public interface TransactionStructMaker {
    String DEBEZIUM_TRANSACTION_KEY = "transaction";
    String DEBEZIUM_TRANSACTION_ID_KEY = "id";
    String DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY = "total_order";
    String DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY = "data_collection_order";
    String DEBEZIUM_TRANSACTION_STATUS_KEY = "status";
    String DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY = "event_count";
    String DEBEZIUM_TRANSACTION_COLLECTION_KEY = "data_collection";
    String DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY = "data_collections";
    String DEBEZIUM_TRANSACTION_TS_MS = "ts_ms";

    void setSchemaNameAdjuster(SchemaNameAdjuster adjuster);

    Struct prepareTxStruct(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value);

    Struct prepareTxEndValue(OffsetContext offsetContext, Instant timestamp);

    Struct prepareTxBeginValue(OffsetContext offsetContext, Instant timestamp);

    Struct prepareTxKey(OffsetContext offsetContext);

    Schema getTransactionBlockSchema();

    Schema getEventCountPerDataCollectionSchema();

    Schema getTransactionKeySchema();

    Schema getTransactionValueSchema();
}
