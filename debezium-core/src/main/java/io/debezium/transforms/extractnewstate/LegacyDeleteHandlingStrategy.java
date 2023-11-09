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
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;

/**
 * Deprecated, use {@link DefaultDeleteHandlingStrategy} instead
 *
 * @author Harvey Yue
 */
@Deprecated
public class LegacyDeleteHandlingStrategy<R extends ConnectRecord<R>> extends AbstractExtractRecordStrategy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LegacyDeleteHandlingStrategy.class);
    private final DeleteHandling deleteHandling;
    private final boolean dropTombstones;

    public LegacyDeleteHandlingStrategy(DeleteHandling deleteHandling, boolean dropTombstones) {
        this.deleteHandling = deleteHandling;
        this.dropTombstones = dropTombstones;
    }

    @Override
    public R handleTruncateRecord(R record) {
        if (dropTombstones) {
            LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
            return null;
        }
        return record;
    }

    @Override
    public R handleDeleteRecord(R record) {
        switch (deleteHandling) {
            case DROP:
                LOGGER.trace("Delete message {} requested to be dropped", record.key());
                return null;
            case NONE:
                // NOTE
                // Debezium TOMBSTONE has both value and valueSchema to null, instead here we are generating
                // a record only with null value that by JDBC connector is treated as a flattened delete.
                // Any change to this behavior can have impact on JDBC connector.
                return afterDelegate.apply(record);
            case REWRITE:
                LOGGER.trace("Delete message {} requested to be rewritten", record.key());
                R oldRecord = beforeDelegate.apply(record);
                if (oldRecord.value() instanceof Struct) {
                    return removedDelegate.apply(oldRecord);
                }
                return oldRecord;
            default:
                throw new DebeziumException("Unknown delete handling mode: " + deleteHandling);
        }
    }

    @Override
    public R handleRecord(R record) {
        R newRecord = afterDelegate.apply(record);
        if (deleteHandling == DeleteHandling.REWRITE) {
            LOGGER.trace("Insert/update message {} requested to be rewritten", record.key());
            if (newRecord.value() instanceof Struct) {
                return updatedDelegate.apply(newRecord);
            }
        }
        return newRecord;
    }

    @Override
    public boolean isRewriteMode() {
        return deleteHandling == DeleteHandling.REWRITE;
    }
}
