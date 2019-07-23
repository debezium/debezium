/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link MessageDecoder} that all decoders should inherit from.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageDecoder.class);

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
