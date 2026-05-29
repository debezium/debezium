/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the current read position within the ${connectorName} data source.
 *
 * <p>The offset map returned by {@link #getOffset()} is persisted by the Kafka Connect
 * framework and used to resume after a restart. Replace the placeholder implementation
 * with the fields meaningful to your connector (e.g. file position, LSN, sequence ID).
 */
public class ${connectorName}OffsetContext extends CommonOffsetContext<${connectorName}SourceInfo> {

    static final String POSITION_KEY = "position";

    private long position;

    public ${connectorName}OffsetContext(${connectorName}SourceInfo sourceInfo) {
        super(sourceInfo);
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public Map<String, ?> getOffset() {
        return Map.of(POSITION_KEY, position);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        // Update sourceInfo with the current position before each event is enqueued.
    }

    @Override
    public TransactionContext getTransactionContext() {
        return new TransactionContext();
    }
}
