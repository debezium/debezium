/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class DefaultTransactionStructMakerTest {

    @Test
    public void getTransactionBlockSchema() {
        Schema expectedSchema = DefaultTransactionStructMaker.TRANSACTION_BLOCK_SCHEMA;
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(Configuration.empty());
        assertThat(transactionStructMaker.getTransactionBlockSchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getEventCountPerDataCollectionSchema() {
        Schema expectedSchema = AbstractTransactionStructMaker.EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA;
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(Configuration.empty());
        assertThat(transactionStructMaker.getEventCountPerDataCollectionSchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getTransactionKeySchema() {
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.NO_OP;
        Schema expectedSchema = SchemaFactory.get().transactionKeySchema(adjuster);
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(Configuration.empty());
        assertThat(transactionStructMaker.getTransactionKeySchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getTransactionValueSchema() {
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.NO_OP;
        Schema expectedSchema = SchemaFactory.get().transactionValueSchema(adjuster);
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(Configuration.empty());
        assertThat(transactionStructMaker.getTransactionValueSchema()).isEqualTo(expectedSchema);
    }
}
