/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class BasicTransactionStructMakerTest {

    @Test
    public void getTransactionBlockSchema() {
        Schema expectedSchema = BasicTransactionStructMaker.TRANSACTION_BLOCK_SCHEMA;
        BasicTransactionStructMaker transactionStructMaker = new BasicTransactionStructMaker();
        assertThat(transactionStructMaker.getTransactionBlockSchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getEventCountPerDataCollectionSchema() {
        Schema expectedSchema = AbstractTransactionStructMaker.EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA;
        BasicTransactionStructMaker transactionStructMaker = new BasicTransactionStructMaker();
        assertThat(transactionStructMaker.getEventCountPerDataCollectionSchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getTransactionKeySchema() {
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.NO_OP;
        Schema expectedSchema = SchemaFactory.get().transactionKeySchema(adjuster);
        BasicTransactionStructMaker transactionStructMaker = new BasicTransactionStructMaker();
        transactionStructMaker.setSchemaNameAdjuster(adjuster);
        assertThat(transactionStructMaker.getTransactionKeySchema()).isEqualTo(expectedSchema);
    }

    @Test
    public void getTransactionValueSchema() {
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.NO_OP;
        Schema expectedSchema = SchemaFactory.get().transactionValueSchema(adjuster);
        BasicTransactionStructMaker transactionStructMaker = new BasicTransactionStructMaker();
        transactionStructMaker.setSchemaNameAdjuster(adjuster);
        assertThat(transactionStructMaker.getTransactionValueSchema()).isEqualTo(expectedSchema);
    }
}
