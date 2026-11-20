/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * An offset context whose offset consists of a single position and the transaction context, for tests of the offsets
 * that records carry.
 */
public class PositionOffsetContext extends CommonOffsetContext<BaseSourceInfo> {

    public static final String POSITION_KEY = "position";

    private final TransactionContext transactionContext;
    private long position;

    public PositionOffsetContext(long position) {
        this(position, new TransactionContext());
    }

    public PositionOffsetContext(long position, TransactionContext transactionContext) {
        super(null);
        this.position = position;
        this.transactionContext = transactionContext;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public Map<String, ?> getOffset() {
        final Map<String, Object> offset = new HashMap<>();
        offset.put(POSITION_KEY, position);
        return transactionContext.store(offset);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return null;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
