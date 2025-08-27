/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Abstract implementation of {@link MessageDecoder} that all decoders should inherit from.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageDecoder.class);
    private static final Duration LOG_INTERVAL_DURATION = Duration.ofSeconds(10);

    // timer needs to be initialized when the first logging attempt happens
    private Timer timer = null;

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        // if message is empty pass control right to ReplicationMessageProcessor to update WAL position info
        if (buffer == null) {
            processor.process(null, null);
        }
        else {
            processNotEmptyMessage(buffer, processor, typeRegistry);
        }
    }

    protected abstract void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException;

    public abstract boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, PositionLocator walPosition);

    public boolean isMessageAlreadyProcessed(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, PositionLocator walPosition, Long beginMessageTransactionId) {
        // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
        // but for the first record seen ever it is possible we received the same LSN as the one obtained from replication slot
        if (walPosition.skipMessage(lastReceivedLsn, beginMessageTransactionId)) {
            if (timerPermitsLogging()) {
                LOGGER.info("Streaming requested from LSN {}, received LSN {} identified as already processed", startLsn, lastReceivedLsn);
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() {
    }

    private boolean timerPermitsLogging() {
        // the first message should always be logged
        if (timer == null || timer.expired()) {
            timer = Threads.timer(Clock.SYSTEM, LOG_INTERVAL_DURATION);
            return true;
        }
        return false;
    }
}
