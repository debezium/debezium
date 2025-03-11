/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.extractnewstate;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteTombstoneHandling;
import io.debezium.util.Loggings;

/**
 * A default implementation of {@link AbstractExtractRecordStrategy}
 *
 * @author Harvey Yue
 */
public class DefaultDeleteHandlingStrategy<R extends ConnectRecord<R>> extends AbstractExtractRecordStrategy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDeleteHandlingStrategy.class);
    private final DeleteTombstoneHandling deleteTombstoneHandling;

    public DefaultDeleteHandlingStrategy(DeleteTombstoneHandling deleteTombstoneHandling, boolean replaceNullWithDefault) {
        super(replaceNullWithDefault);
        this.deleteTombstoneHandling = deleteTombstoneHandling;
    }

    @Override
    public R handleTombstoneRecord(R record) {
        switch (deleteTombstoneHandling) {
            case DROP:
            case TOMBSTONE:
            case REWRITE:
                Loggings.logTraceAndTraceRecord(LOGGER, record.key(), "Tombstone record arrived and requested to be dropped");
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
                Loggings.logTraceAndTraceRecord(LOGGER, record.key(), "Delete message requested to be dropped");
                return null;
            case TOMBSTONE:
                // NOTE
                // Debezium TOMBSTONE has both value and valueSchema to null, instead here we are generating
                // a record only with null value that by JDBC connector is treated as a flattened delete.
                // Any change to this behavior can have impact on JDBC connector.
                return afterDelegate.apply(record);
            case REWRITE:
            case REWRITE_WITH_TOMBSTONE:
                Loggings.logTraceAndTraceRecord(LOGGER, record.key(), "Delete message requested to be rewritten");
                R oldRecord = beforeDelegate.apply(record);
                // need to add the rewrite "__deleted" field manually since mongodb's value is a string type
                if (oldRecord.value() instanceof Struct) {
                    return removedDelegate.apply(oldRecord);
                }
                return oldRecord;
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
                Loggings.logTraceAndTraceRecord(LOGGER, record.key(), "Insert/update message requested to be rewritten");
                // need to add the rewrite "__deleted" field manually since mongodb's value is a string type
                if (newRecord.value() instanceof Struct) {
                    return updatedDelegate.apply(newRecord);
                }
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
