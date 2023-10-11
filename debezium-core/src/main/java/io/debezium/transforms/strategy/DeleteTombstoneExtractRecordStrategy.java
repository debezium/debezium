/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.strategy;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteTombstoneHandling;

/**
 * A default implementation of {@link AbstractExtractRecordStrategy}
 *
 * @author Harvey Yue
 */
public class DeleteTombstoneExtractRecordStrategy<R extends ConnectRecord<R>> extends AbstractExtractRecordStrategy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTombstoneExtractRecordStrategy.class);
    private final DeleteTombstoneHandling deleteTombstoneHandling;

    public DeleteTombstoneExtractRecordStrategy(DeleteTombstoneHandling deleteTombstoneHandling) {
        this.deleteTombstoneHandling = deleteTombstoneHandling;
    }

    @Override
    public R handleTruncateRecord(R record) {
        switch (deleteTombstoneHandling) {
            case DROP:
            case TOMBSTONE:
            case REWRITE:
                LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            case REWRITE_WITH_TOMBSTONE:
                return record;
            default:
                throw new DebeziumException("Unknown delete handling mode: " + deleteTombstoneHandling);
        }
    }

    @Override
    public R handleDeleteRecord(R record) {
        switch (deleteTombstoneHandling) {
            case DROP:
                LOGGER.trace("Delete message {} requested to be dropped", record.key());
                return null;
            case TOMBSTONE:
                return afterDelegate.apply(record);
            case REWRITE:
            case REWRITE_WITH_TOMBSTONE:
                LOGGER.trace("Delete message {} requested to be rewritten", record.key());
                R oldRecord = beforeDelegate.apply(record);
                return removedDelegate.apply(oldRecord);
            default:
                throw new DebeziumException("Unknown delete handling mode: " + deleteTombstoneHandling);
        }
    }

    @Override
    public R handleRecord(R record) {
        R newRecord = afterDelegate.apply(record);
        switch (deleteTombstoneHandling) {
            case REWRITE:
            case REWRITE_WITH_TOMBSTONE:
                LOGGER.trace("Insert/update message {} requested to be rewritten", record.key());
                return updatedDelegate.apply(newRecord);
            default:
                return newRecord;
        }
    }

    @Override
    public boolean isRewriteMode() {
        return deleteTombstoneHandling == DeleteTombstoneHandling.REWRITE
                || deleteTombstoneHandling == DeleteTombstoneHandling.REWRITE_WITH_TOMBSTONE;
    }
}
