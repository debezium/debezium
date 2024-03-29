/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The context holds internal state necessary for book-keeping of events in active transaction.
 * The main data tracked are
 * <ul>
 * <li>active transaction id</li>
 * <li>the total event number seen from the transaction</li>
 * <li>the number of events per table/collection seen in the transaction</li>
 * </ul>
 *
 * The state of this context is stored in offsets and is recovered upon restart.
 *
 * @author Jiri Pechanec
 */
@NotThreadSafe
public class TransactionContext {

    public static final String OFFSET_TRANSACTION_ID = TransactionStructMaker.DEBEZIUM_TRANSACTION_KEY + "_" + TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY;
    private static final String OFFSET_TABLE_COUNT_PREFIX = TransactionStructMaker.DEBEZIUM_TRANSACTION_KEY + "_"
            + TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY + "_";
    private static final int OFFSET_TABLE_COUNT_PREFIX_LENGTH = OFFSET_TABLE_COUNT_PREFIX.length();

    public String transactionId = null;
    public final Map<String, Long> perTableEventCount = new HashMap<>();
    public final Map<String, Long> viewPerTableEventCount = Collections.unmodifiableMap(perTableEventCount);
    public long totalEventCount = 0;

    private void reset() {
        transactionId = null;
        totalEventCount = 0;
        perTableEventCount.clear();
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        if (!Objects.isNull(transactionId)) {
            offset.put(OFFSET_TRANSACTION_ID, transactionId);
        }
        final String tableCountPrefix = OFFSET_TABLE_COUNT_PREFIX;
        for (final Entry<String, Long> e : perTableEventCount.entrySet()) {
            offset.put(tableCountPrefix + e.getKey(), e.getValue());
        }
        return offset;
    }

    @SuppressWarnings("unchecked")
    public static TransactionContext load(Map<String, ?> offsets) {
        final Map<String, Object> o = (Map<String, Object>) offsets;
        final TransactionContext context = new TransactionContext();

        context.transactionId = (String) o.get(OFFSET_TRANSACTION_ID);

        for (final Entry<String, Object> offset : o.entrySet()) {
            if (offset.getKey().startsWith(OFFSET_TABLE_COUNT_PREFIX)) {
                final String dataCollectionId = offset.getKey().substring(OFFSET_TABLE_COUNT_PREFIX_LENGTH);
                final Long count = (Long) offset.getValue();
                context.perTableEventCount.put(dataCollectionId, count);
            }
        }

        context.totalEventCount = context.perTableEventCount.values().stream().mapToLong(x -> x).sum();

        return context;
    }

    public boolean isTransactionInProgress() {
        return transactionId != null;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public long getTotalEventCount() {
        return totalEventCount;
    }

    public void beginTransaction(TransactionInfo transactionInfo) {
        reset();
        transactionId = transactionInfo.getTransactionId();
    }

    public void beginTransaction(String transactionId) {
        // Needed for backward compatibility where other connectors directly call/interact with beginTransaction
        beginTransaction(new BasicTransactionInfo(transactionId));
    }

    public void endTransaction() {
        reset();
    }

    public long event(DataCollectionId source) {
        totalEventCount++;
        final String sourceName = source.toString();
        final long dataCollectionEventOrder = perTableEventCount.getOrDefault(sourceName, 0L).longValue() + 1;
        perTableEventCount.put(sourceName, Long.valueOf(dataCollectionEventOrder));
        return dataCollectionEventOrder;
    }

    public Map<String, Long> getPerTableEventCount() {
        return viewPerTableEventCount;
    }

    @Override
    public String toString() {
        return "TransactionContext [currentTransactionId=" + transactionId + ", perTableEventCount="
                + perTableEventCount + ", totalEventCount=" + totalEventCount + "]";
    }
}
