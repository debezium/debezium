/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;

/**
 * Unit tests for {@link TransactionMessage}, in particular the transaction final (commit) LSN
 * carried on the pgoutput BEGIN message.
 */
public class TransactionMessageTest {

    @Test
    void beginMessageExposesFinalLsn() {
        final Lsn finalLsn = Lsn.valueOf(5005117783192L);
        final TransactionMessage message = new TransactionMessage(Operation.BEGIN, 1234L, Instant.EPOCH, finalLsn);

        assertThat(message.getOperation()).isEqualTo(Operation.BEGIN);
        assertThat(message.getFinalLsn()).isEqualTo(finalLsn);
    }

    @Test
    void legacyConstructorLeavesFinalLsnNull() {
        // The 3-argument constructor is kept for backward compatibility and must not expose a final LSN.
        final TransactionMessage message = new TransactionMessage(Operation.COMMIT, 1234L, Instant.EPOCH);

        assertThat(message.getFinalLsn()).isNull();
    }
}
