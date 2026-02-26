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

/**
 * Interface to define how to make transaction structs, both when adding them to data change events
 * and when creating separate transaction metadata events (e.g., for begin/end)
 */
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

    /**
     * Adds the transaction block to the data change message
     *
     * @param offsetContext current offsetContext used to get transaction metadata
     * @param dataCollectionEventOrder the event order for the data collection of this transaction
     * @param value the data change struct to add the transaction block to
     * @return the updated struct with the transaction block
     */
    Struct addTransactionBlock(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value);

    /**
     * Creates the value struct for a transaction end message
     * @param offsetContext current offsetContext used to get transaction metadata
     * @param timestamp timestamp of the transaction
     * @return end transaction struct
     */
    Struct buildEndTransactionValue(OffsetContext offsetContext, Instant timestamp);

    /**
     * Creates the value struct for a transaction begin message
     * @param offsetContext current offsetContext used to get transaction metadata
     * @param timestamp timestamp of the transaction
     * @return begin transaction struct
     */
    Struct buildBeginTransactionValue(OffsetContext offsetContext, Instant timestamp);

    /**
     * Creates the key struct for the transaction message
     * @param offsetContext current offsetContext used to get transaction metadata
     * @return key transaction struct
     */
    Struct buildTransactionKey(OffsetContext offsetContext);

    /**
     * Get the schema for the transaction block in a data change event
     * @return
     */
    Schema getTransactionBlockSchema();

    /**
     * Get the schema for the event count per data collection
     * @return
     */
    Schema getEventCountPerDataCollectionSchema();

    /**
     * Get the schema for the transaction key of a begin/end event
     * @return
     */
    Schema getTransactionKeySchema();

    /**
     * Get the schema for the transaction value of a begin/end event
     * @return
     */
    Schema getTransactionValueSchema();
}
