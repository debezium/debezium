/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import org.apache.kafka.connect.data.Schema;

import io.debezium.config.Configuration;
import io.debezium.schema.SchemaFactory;

public class DefaultTransactionStructMaker extends AbstractTransactionStructMaker implements TransactionStructMaker {

    public static final Schema TRANSACTION_BLOCK_SCHEMA = SchemaFactory.get().transactionBlockSchema();

    public DefaultTransactionStructMaker(Configuration config) {
        super(config);
    }

    @Override
    public Schema getTransactionBlockSchema() {
        return TRANSACTION_BLOCK_SCHEMA;
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
