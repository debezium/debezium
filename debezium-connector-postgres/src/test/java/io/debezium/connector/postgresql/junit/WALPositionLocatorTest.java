/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Optional;

import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.connector.postgresql.connection.WalPositionLocator;

/**
 * @author Pranav Tiwari
 *
 * Comprehensive test coverage for WalPositionLocator class.
 * Tests every line and branch to ensure 100% code coverage.
 * 
 * IMPORTANT: WalPositionLocator follows a two-phase workflow:
 * 
 * PHASE 1 - SEARCH/LOCATION PHASE:
 * - Multiple calls to resumeFromLsn() to search for the restart point
 * - Each LSN gets added to lsnSeen set during this phase
 * - Continue until resumeFromLsn() returns Optional.of(lsn) (found restart point)
 * 
 * PHASE 2 - STREAMING PHASE:
 * - Call enableFiltering() to switch to streaming mode
 * - Only call skipMessage() to check if streaming LSNs should be skipped
 * - skipMessage() compares against LSNs stored during the search phase
 */

public class WALPositionLocatorTest {

    // =============================================================================
    // Constructor Tests
    // =============================================================================
    @Test
    public void testDefaultConstructor() {
        WalPositionLocator locator = new WalPositionLocator();

        assertThat(locator.getLastCommitStoredLsn()).isNull();
        assertThat(locator.getLastEventStoredLsn()).isNull();
        assertThat(locator.searchingEnabled()).isFalse();
    }

    @Test
    public void testParameterizedConstructor() {
        Lsn lastCommit = Lsn.valueOf(100L);
        Lsn lastEvent = Lsn.valueOf(150L);
        ReplicationMessage.Operation lastType = ReplicationMessage.Operation.COMMIT;

        WalPositionLocator locator = new WalPositionLocator(lastCommit, lastEvent, lastType);

        assertThat(locator.getLastCommitStoredLsn()).isEqualTo(lastCommit);
        assertThat(locator.getLastEventStoredLsn()).isEqualTo(lastEvent);
        assertThat(locator.searchingEnabled()).isTrue();
    }

    @Test
    public void whenLastCommitStoredLsnIsNull_shouldBeginProcessingOfMessageFromTheFirstLsn() {
        WalPositionLocator locator = new WalPositionLocator();
        Lsn beginLsn = Lsn.valueOf(100L);
        ReplicationMessage message = createBeginMessage(1L);
        Optional<Lsn> result = locator.resumeFromLsn(beginLsn, message);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(beginLsn);
        assertThat(locator.skipMessage(beginLsn)).isFalse();
    }

    @Test
    public void whenLastCommitStoredIsLessThanCurrentCommit_shouldBeginProcessingMessageFromBeginLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, ReplicationMessage.Operation.COMMIT);

        Lsn nextBeginLsn = Lsn.valueOf(110L);
        Lsn nextCommitLsn = Lsn.valueOf(120L);
        ReplicationMessage beginMessage = createBeginMessage(1L);
        ReplicationMessage insertMessage = createInsertMessage(2L);
        ReplicationMessage commitMessage = createCommitMessage(3L);

        Optional<Lsn> beginResultLsn = locator.resumeFromLsn(nextBeginLsn, beginMessage);
        Optional<Lsn> insertResultLsn = locator.resumeFromLsn(nextBeginLsn, insertMessage);
        Optional<Lsn> commitResultLsn = locator.resumeFromLsn(nextCommitLsn, commitMessage);

        assertThat(beginResultLsn).isEmpty();
        assertThat(insertResultLsn).isEmpty();
        assertThat(commitResultLsn).isNotEmpty();
        assertThat(commitResultLsn.get()).isEqualTo(nextBeginLsn);

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextBeginLsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenLastCommitStoredIsLessThanCurrentCommitAndFirstMessageIsNotBegin_shouldBeginProcessingMessageFromTheFirstLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, ReplicationMessage.Operation.COMMIT);

        Lsn nextInsertLsn = Lsn.valueOf(110L);
        Lsn nextCommitLsn = Lsn.valueOf(120L);
        ReplicationMessage insertMessage = createInsertMessage(2L);
        ReplicationMessage commitMessage = createCommitMessage(3L);

        Optional<Lsn> insertResultLsn = locator.resumeFromLsn(nextInsertLsn, insertMessage);
        Optional<Lsn> commitResultLsn = locator.resumeFromLsn(nextCommitLsn, commitMessage);

        assertThat(insertResultLsn).isEmpty();
        assertThat(commitResultLsn).isNotEmpty();
        assertThat(commitResultLsn.get()).isEqualTo(nextInsertLsn);

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextInsertLsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenStoreLsnAfterLastEventStoredLsnFound_shouldBeginProcessingMessageFromTheLsnAfterLastEventStoredLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, ReplicationMessage.Operation.COMMIT);

        Lsn nextBeginLsn = Lsn.valueOf(110L);
        Lsn insertMessage1Lsn = nextBeginLsn;
        Lsn insertMessage2Lsn = Lsn.valueOf(120L);
        Lsn insertMessage3Lsn = Lsn.valueOf(130L);
        Lsn insertMessage4Lsn = Lsn.valueOf(140L);
        Lsn insertMessage5Lsn = Lsn.valueOf(150L);
        Lsn insertMessage6Lsn = Lsn.valueOf(160L);
        Lsn nextCommitLsn = Lsn.valueOf(170L);

        ReplicationMessage beginMessage = createBeginMessage(1L);
        ReplicationMessage insertMessage1 = createInsertMessage(2L);
        ReplicationMessage insertMessage2 = createInsertMessage(3L);
        ReplicationMessage insertMessage3 = createInsertMessage(4L);
        ReplicationMessage insertMessage4 = createInsertMessage(5L);
        ReplicationMessage insertMessage5 = createInsertMessage(6L);
        ReplicationMessage insertMessage6 = createInsertMessage(7L);
        ReplicationMessage commitMessage = createCommitMessage(8L);

        Optional<Lsn> beginResultLsn = locator.resumeFromLsn(nextBeginLsn, beginMessage);
        Optional<Lsn> insertMessage1ResultLsn = locator.resumeFromLsn(insertMessage1Lsn, insertMessage1);
        Optional<Lsn> insertMessage2ResultLsn = locator.resumeFromLsn(insertMessage2Lsn, insertMessage2);
        Optional<Lsn> insertMessage3ResultLsn = locator.resumeFromLsn(insertMessage3Lsn, insertMessage3);
        Optional<Lsn> insertMessage4ResultLsn = locator.resumeFromLsn(insertMessage4Lsn, insertMessage4);
        Optional<Lsn> insertMessage5ResultLsn = locator.resumeFromLsn(insertMessage5Lsn, insertMessage5);

        assertThat(beginResultLsn).isEmpty();
        assertThat(insertMessage1ResultLsn).isEmpty();
        assertThat(insertMessage2ResultLsn).isEmpty();
        assertThat(insertMessage3ResultLsn).isEmpty();
        assertThat(insertMessage4ResultLsn).isEmpty();
        assertThat(insertMessage5ResultLsn).isNotEmpty();

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextBeginLsn)).isTrue();
        assertThat(locator.skipMessage(insertMessage1Lsn)).isTrue();
        assertThat(locator.skipMessage(insertMessage2Lsn)).isTrue();
        assertThat(locator.skipMessage(insertMessage3Lsn)).isTrue();
        assertThat(locator.skipMessage(insertMessage4Lsn)).isTrue();
        assertThat(locator.skipMessage(insertMessage5Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage6Lsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsNull_shouldBeginProcessingMessageFromTxStartLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(110L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, null);

        Lsn nextBeginLsn = Lsn.valueOf(110L);
        Lsn insertMessage1Lsn = nextBeginLsn;
        Lsn insertMessage2Lsn = Lsn.valueOf(120L);
        Lsn insertMessage3Lsn = Lsn.valueOf(130L);
        Lsn insertMessage4Lsn = Lsn.valueOf(140L);
        Lsn insertMessage5Lsn = Lsn.valueOf(150L);
        Lsn insertMessage6Lsn = Lsn.valueOf(160L);
        Lsn nextCommitLsn = Lsn.valueOf(170L);

        ReplicationMessage beginMessage = createBeginMessage(1L);
        ReplicationMessage insertMessage1 = createInsertMessage(2L);

        Optional<Lsn> beginResultLsn = locator.resumeFromLsn(nextBeginLsn, beginMessage);
        Optional<Lsn> insertMessage1ResultLsn = locator.resumeFromLsn(insertMessage1Lsn, insertMessage1);

        assertThat(beginResultLsn).isEmpty();
        assertThat(insertMessage1ResultLsn).isNotEmpty();

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextBeginLsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage1Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage2Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage3Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage4Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage5Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage6Lsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsBegin_shouldBeginProcessingMessageFromTxStartLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(110L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, ReplicationMessage.Operation.BEGIN);

        Lsn nextBeginLsn = Lsn.valueOf(110L);
        Lsn insertMessage1Lsn = nextBeginLsn;
        Lsn insertMessage2Lsn = Lsn.valueOf(120L);
        Lsn insertMessage3Lsn = Lsn.valueOf(130L);
        Lsn insertMessage4Lsn = Lsn.valueOf(140L);
        Lsn insertMessage5Lsn = Lsn.valueOf(150L);
        Lsn insertMessage6Lsn = Lsn.valueOf(160L);
        Lsn nextCommitLsn = Lsn.valueOf(170L);

        ReplicationMessage beginMessage = createBeginMessage(1L);
        ReplicationMessage insertMessage1 = createInsertMessage(2L);

        Optional<Lsn> beginResultLsn = locator.resumeFromLsn(nextBeginLsn, beginMessage);
        Optional<Lsn> insertMessage1ResultLsn = locator.resumeFromLsn(insertMessage1Lsn, insertMessage1);

        assertThat(beginResultLsn).isEmpty();
        assertThat(insertMessage1ResultLsn).isNotEmpty();

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextBeginLsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage1Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage2Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage3Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage4Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage5Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage6Lsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsCommit_shouldBeginProcessingMessageFromTxStartLsn() {
        Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        Lsn lastEventStoredLsn = Lsn.valueOf(110L);
        WalPositionLocator locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn, ReplicationMessage.Operation.COMMIT);

        Lsn nextBeginLsn = Lsn.valueOf(110L);
        Lsn insertMessage1Lsn = nextBeginLsn;
        Lsn insertMessage2Lsn = Lsn.valueOf(120L);
        Lsn insertMessage3Lsn = Lsn.valueOf(130L);
        Lsn insertMessage4Lsn = Lsn.valueOf(140L);
        Lsn insertMessage5Lsn = Lsn.valueOf(150L);
        Lsn insertMessage6Lsn = Lsn.valueOf(160L);
        Lsn nextCommitLsn = Lsn.valueOf(170L);

        ReplicationMessage beginMessage = createBeginMessage(1L);
        ReplicationMessage insertMessage1 = createInsertMessage(2L);

        Optional<Lsn> beginResultLsn = locator.resumeFromLsn(nextBeginLsn, beginMessage);
        Optional<Lsn> insertMessage1ResultLsn = locator.resumeFromLsn(insertMessage1Lsn, insertMessage1);

        assertThat(beginResultLsn).isEmpty();
        assertThat(insertMessage1ResultLsn).isNotEmpty();

        locator.enableFiltering();
        assertThat(locator.skipMessage(nextBeginLsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage1Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage2Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage3Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage4Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage5Lsn)).isFalse();
        assertThat(locator.skipMessage(insertMessage6Lsn)).isFalse();
        assertThat(locator.skipMessage(nextCommitLsn)).isFalse();
    }

    @Test
    public void whenReceivedALsnWhichIsValidButNoSeen_shouldThrowException() {
        WalPositionLocator locator = new WalPositionLocator();
        Lsn searchLsn = Lsn.valueOf(100L);
        Lsn unseenLsn = Lsn.valueOf(200L); // Valid LSN not seen during search

        locator.resumeFromLsn(searchLsn, createBeginMessage(1L));

        locator.enableFiltering();

        assertThatThrownBy(() -> locator.skipMessage(unseenLsn))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Message with LSN")
                .hasMessageContaining("not present among LSNs seen");
    }

    // =============================================================================
    // toString Tests
    // =============================================================================

    @Test
    public void testToString_defaultConstructor() {
        WalPositionLocator locator = new WalPositionLocator();

        String result = locator.toString();

        assertThat(result).contains("WalPositionLocator")
                .contains("lastCommitStoredLsn=null")
                .contains("lastEventStoredLsn=null")
                .contains("lastProcessedMessageType=null")
                .contains("txStartLsn=null")
                .contains("lsnAfterLastEventStoredLsn=null")
                .contains("firstLsnReceived=null")
                .contains("passMessages=true")
                .contains("startStreamingLsn=null")
                .contains("storeLsnAfterLastEventStoredLsn=false");
    }

    @Test
    public void testToString_parameterizedConstructor() {
        Lsn lastCommit = Lsn.valueOf(100L);
        Lsn lastEvent = Lsn.valueOf(150L);
        ReplicationMessage.Operation lastType = ReplicationMessage.Operation.COMMIT;
        WalPositionLocator locator = new WalPositionLocator(lastCommit, lastEvent, lastType);

        String result = locator.toString();

        assertThat(result).contains("WalPositionLocator")
                .contains("lastCommitStoredLsn=" + lastCommit)
                .contains("lastEventStoredLsn=" + lastEvent)
                .contains("lastProcessedMessageType=" + lastType);
    }

    @Test
    public void testToString_afterProcessingMessages() {
        WalPositionLocator locator = new WalPositionLocator();
        Lsn lsn = Lsn.valueOf(100L);
        locator.resumeFromLsn(lsn, createBeginMessage(1L));

        String result = locator.toString();

        assertThat(result).contains("firstLsnReceived=" + lsn)
                .contains("startStreamingLsn=" + lsn);
    }

    // =============================================================================
    // Helper Methods
    // =============================================================================

    private ReplicationMessage createBeginMessage(long txId) {
        return new TransactionMessage(ReplicationMessage.Operation.BEGIN, txId, Instant.now());
    }

    private ReplicationMessage createCommitMessage(long txId) {
        return new TransactionMessage(ReplicationMessage.Operation.COMMIT, txId, Instant.now());
    }

    private ReplicationMessage createInsertMessage(long txId) {
        return new ReplicationMessage.NoopMessage(txId, Instant.now(), ReplicationMessage.Operation.INSERT);
    }
}
