/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;

/**
 * Abstract implementation of {@link MessageDecoder} that all decoders should inherit from.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageDecoder.class);

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        // if message is empty pass control right to ReplicationMessageProcessor to update WAL position info
        if (buffer == null) {
            processor.process(null);
        }
        else {
            processNotEmptyMessage(buffer, processor, typeRegistry);
        }
    }

    protected abstract void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException;

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Long lastReceivedLsn, Long startLsn, boolean skipFirstFlushRecord) {
        // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
        // but for the first record seen ever it is possible we received the same LSN as the one obtained from replication slot
        if (startLsn.compareTo(lastReceivedLsn) > 0 || (startLsn.equals(lastReceivedLsn) && skipFirstFlushRecord)) {
            LOGGER.info("Streaming requested from LSN {} but received LSN {} that is same or smaller so skipping the message", startLsn, lastReceivedLsn);
            return true;
        }
        return false;
    }
}
