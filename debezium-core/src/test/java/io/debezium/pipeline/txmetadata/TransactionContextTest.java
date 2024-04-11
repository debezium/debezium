/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

public class TransactionContextTest {

    @Test
    public void testGetTransactionId() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.getTransactionId()).isEqualTo(txId);
    }

    @Test
    public void store() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        Map offsets = new HashMap();
        Map actualOffsets = context.store(offsets);
        assertThat(actualOffsets).isEqualTo(Map.of(TransactionContext.OFFSET_TRANSACTION_ID, txId));
    }

    @Test
    public void load() {
        String expectedId = "foo";
        Map offsets = Map.of(TransactionContext.OFFSET_TRANSACTION_ID, expectedId);
        TransactionContext context = TransactionContext.load(offsets);
        assertThat(context.getTransactionId()).isEqualTo(expectedId);
    }

    @Test
    public void isTransactionInProgress() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.isTransactionInProgress()).isEqualTo(true);
        context.endTransaction();
        assertThat(context.isTransactionInProgress()).isEqualTo(false);
    }

    @Test
    public void getTotalEventCount() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.getTotalEventCount()).isEqualTo(0);
        context.event(new TableId("catalog", "schema", "table"));
        assertThat(context.getTotalEventCount()).isEqualTo(1);
    }

    @Test
    public void endTransaction() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.getTransactionId()).isEqualTo(txId);
        context.endTransaction();
        assertThat(context.getTransactionId()).isEqualTo(null);
    }

    @Test
    public void getPerTableEventCount() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new DefaultTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.getTotalEventCount()).isEqualTo(0);
        DataCollectionId id1 = new TableId("catalog", "schema", "table1");
        DataCollectionId id2 = new TableId("catalog", "schema", "table2");
        context.event(id1);
        context.event(id1);
        context.event(id1);
        context.event(id2);
        context.event(id2);
        assertThat(context.getPerTableEventCount()).isEqualTo(Map.of(id1.toString(), 3L, id2.toString(), 2L));
    }

    @Test
    public void testToString() {
        TransactionContext context = new TransactionContext();
        TransactionInfo info = new DefaultTransactionInfo("foo");
        context.beginTransaction(info);
        assertThat(context.toString()).isEqualTo("TransactionContext [currentTransactionId=foo, perTableEventCount={}, totalEventCount=0]");
    }
}
