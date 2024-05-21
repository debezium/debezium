/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
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
    public void getStructWithTransactionBlock() {
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(Configuration.empty());
        TransactionContext transactionContext = new TransactionContext();
        String expectedId = "tx_id";
        long expectedTotalEventCount = 2L;
        transactionContext.setTransactionId(expectedId);
        transactionContext.setTotalEventCount(expectedTotalEventCount);
        OffsetContext offsetContext = mock(OffsetContext.class);
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);
        long expectedDataCollectionOrder = 1L;
        Struct inputStruct = null;
        Struct expectedStruct = new Struct(transactionStructMaker.getTransactionBlockSchema());
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, expectedId);
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, expectedTotalEventCount);
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, expectedDataCollectionOrder);
        assertThat(transactionStructMaker.addTransactionBlock(offsetContext, expectedDataCollectionOrder, inputStruct)).isEqualTo(expectedStruct);
    }

    @Test
    public void getStructWithTransactionBlockExcludingTxId() {
        Configuration config = Configuration.from(Map.of(CommonConnectorConfig.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "ID"));
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(config);
        TransactionContext transactionContext = new TransactionContext();
        String inputTxId = "tx_id";
        long expectedTotalEventCount = 2L;
        transactionContext.setTransactionId(inputTxId);
        transactionContext.setTotalEventCount(expectedTotalEventCount);
        OffsetContext offsetContext = mock(OffsetContext.class);
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);
        long expectedDataCollectionOrder = 1L;
        Struct inputStruct = null;
        Struct expectedStruct = new Struct(transactionStructMaker.getTransactionBlockSchema());
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, expectedTotalEventCount);
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, expectedDataCollectionOrder);
        assertThat(transactionStructMaker.addTransactionBlock(offsetContext, expectedDataCollectionOrder, inputStruct)).isEqualTo(expectedStruct);
    }

    @Test
    public void getStructWithTransactionBlockExcludingOrder() {
        Configuration config = Configuration.from(Map.of(CommonConnectorConfig.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "ORDER"));
        DefaultTransactionStructMaker transactionStructMaker = new DefaultTransactionStructMaker(config);
        TransactionContext transactionContext = new TransactionContext();
        String inputTxId = "tx_id";
        long expectedTotalEventCount = 2L;
        transactionContext.setTransactionId(inputTxId);
        transactionContext.setTotalEventCount(expectedTotalEventCount);
        OffsetContext offsetContext = mock(OffsetContext.class);
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);
        long expectedDataCollectionOrder = 1L;
        Struct inputStruct = null;
        Struct expectedStruct = new Struct(transactionStructMaker.getTransactionBlockSchema());
        expectedStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, inputTxId);
        assertThat(transactionStructMaker.addTransactionBlock(offsetContext, expectedDataCollectionOrder, inputStruct)).isEqualTo(expectedStruct);
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
