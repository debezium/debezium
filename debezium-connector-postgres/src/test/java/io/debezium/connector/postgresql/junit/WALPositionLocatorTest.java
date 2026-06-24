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

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.doc.FixFor;

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
    void testDefaultConstructor() {
        WalPositionLocator locator = new WalPositionLocator();

        assertThat(locator.getLastCommitStoredLsn()).isNull();
        assertThat(locator.getLastEventStoredLsn()).isNull();
        assertThat(locator.searchingEnabled()).isFalse();
    }

    @Test
    void testParameterizedConstructor() {
        Lsn lastCommit = Lsn.valueOf(100L);
        Lsn lastEvent = Lsn.valueOf(150L);
        ReplicationMessage.Operation lastType = ReplicationMessage.Operation.COMMIT;

        WalPositionLocator locator = new WalPositionLocator(lastCommit, lastEvent, lastType);

        assertThat(locator.getLastCommitStoredLsn()).isEqualTo(lastCommit);
        assertThat(locator.getLastEventStoredLsn()).isEqualTo(lastEvent);
        assertThat(locator.searchingEnabled()).isTrue();
    }

    @Test
    void whenLastCommitStoredLsnIsNullShouldBeginProcessingOfMessageFromTheFirstLsn() {
        WalPositionLocator locator = new WalPositionLocator();
        Lsn beginLsn = Lsn.valueOf(100L);
        ReplicationMessage message = createBeginMessage(1L);
        Optional<Lsn> result = locator.resumeFromLsn(beginLsn, message);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(beginLsn);
        assertThat(locator.skipMessage(beginLsn)).isFalse();
    }

    @Test
    void whenLastCommitStoredIsLessThanCurrentCommitShouldBeginProcessingMessageFromBeginLsn() {
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
    void whenLastCommitStoredIsLessThanCurrentCommitAndFirstMessageIsNotBeginShouldBeginProcessingMessageFromTheFirstLsn() {
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
    void whenStoreLsnAfterLastEventStoredLsnFoundShouldBeginProcessingMessageFromTheLsnAfterLastEventStoredLsn() {
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
    void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsNullShouldBeginProcessingMessageFromTxStartLsn() {
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
    void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsBeginShouldBeginProcessingMessageFromTxStartLsn() {
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
    void whenStoreLsnAfterLastEventStoredLsnFoundAndBeginMessageProcessedAndLastProcessedMessageTypeIsCommitShouldBeginProcessingMessageFromTxStartLsn() {
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
    void whenReceivedALsnWhichIsValidButNoSeenShouldThrowException() {
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
    void testToStringDefaultConstructor() {
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
    void testToStringParameterizedConstructor() {
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
    void testToStringAfterProcessingMessages() {
        WalPositionLocator locator = new WalPositionLocator();
        Lsn lsn = Lsn.valueOf(100L);
        locator.resumeFromLsn(lsn, createBeginMessage(1L));

        String result = locator.toString();

        assertThat(result).contains("firstLsnReceived=" + lsn)
                .contains("startStreamingLsn=" + lsn);
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenMultipleEventsShareSameLsnShouldResumeAfterProcessedCount() {
        // Simulate: 5 events all at LSN 140, we processed 3 before crash
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final long lsnEventsProcessed = 3;
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, lsnEventsProcessed);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn commitLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);
        final var commitMessage = createCommitMessage(2L);

        // Search phase: find the resume position
        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #1
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #2
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #3 (matches count)

        // Event #4 exceeds processed count, resume from stored LSN with skip
        final Optional<Lsn> result = locator.resumeFromLsn(sameLsn, insertMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(sameLsn);

        // Replay phase: enableFiltering then skipMessage should skip first 3 events at this LSN
        locator.enableFiltering();

        // Events before stored LSN are filtered by lsnSeen
        assertThat(locator.skipMessage(beginLsn)).isTrue();

        // First 3 events at stored LSN are skipped (already processed)
        assertThat(locator.skipMessage(sameLsn)).isTrue(); // skip #1
        assertThat(locator.skipMessage(sameLsn)).isTrue(); // skip #2
        assertThat(locator.skipMessage(sameLsn)).isTrue(); // skip #3

        // Events 4 and 5 should pass through (not yet processed)
        assertThat(locator.skipMessage(sameLsn)).isFalse(); // event #4 passes
        assertThat(locator.skipMessage(sameLsn)).isFalse(); // event #5 passes
        assertThat(locator.skipMessage(commitLsn)).isFalse(); // COMMIT passes
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenAllSameLsnEventsProcessedShouldResumeFromNextLsn() {
        // All 3 events at LSN 140 were processed, then crash after COMMIT
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, 3);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn commitLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);
        final var commitMessage = createCommitMessage(2L);

        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #1
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #2
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #3

        // No more events at this LSN, next different LSN arrives
        final Optional<Lsn> result = locator.resumeFromLsn(commitLsn, commitMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(commitLsn);
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenLsnEventsProcessedIsZeroShouldReplayAllEventsAtThatLsn() {
        // Backward compatibility: old offsets without counter (effectively 0)
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, 0);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn nextLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);

        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty();

        // Next different LSN should resume from here (original behavior preserved)
        final Optional<Lsn> result = locator.resumeFromLsn(nextLsn, insertMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(nextLsn);
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenLsnEventsProcessedIsOneShouldUseNormalResumeLogic() {
        // Normal case: single event at each LSN, counter is 1.
        // Should NOT trigger skip logic, should use existing "next different LSN" behavior.
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, 1);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn eventLsn = Lsn.valueOf(140L);
        final Lsn nextLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);

        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(eventLsn, insertMessage)).isEmpty();

        // Next different LSN triggers resume (normal behavior, no skip)
        final Optional<Lsn> result = locator.resumeFromLsn(nextLsn, insertMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(nextLsn);
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenExactlyTwoEventsShareSameLsnShouldSkipCorrectly() {
        // Boundary case: lsnEventsProcessed=2 is the minimum value that triggers
        // the same-LSN skip logic (threshold is > 1).
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, 2);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn commitLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);

        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #1
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #2 (matches count)

        // Event #3 exceeds count, triggers resume
        final Optional<Lsn> result = locator.resumeFromLsn(sameLsn, insertMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(sameLsn);

        locator.enableFiltering();
        assertThat(locator.skipMessage(beginLsn)).isTrue();
        assertThat(locator.skipMessage(sameLsn)).isTrue(); // skip #1
        assertThat(locator.skipMessage(sameLsn)).isTrue(); // skip #2
        assertThat(locator.skipMessage(sameLsn)).isFalse(); // event #3 passes
        assertThat(locator.skipMessage(commitLsn)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenAllSameLsnEventsProcessedAndNoMoreAtThatLsnShouldResumeFromCommit() {
        // Edge case: processed exactly all events at the stored LSN, and the next
        // message is a COMMIT (no unprocessed events remain at that LSN).
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, 2);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn commitLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);
        final var commitMessage = createCommitMessage(2L);

        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #1
        assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty(); // event #2 (matches count)

        // Next message is COMMIT at different LSN: all events at stored LSN were processed,
        // so we resume from the commit LSN (next different LSN path)
        final Optional<Lsn> result = locator.resumeFromLsn(commitLsn, commitMessage);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(commitLsn);
    }

    @Test
    @FixFor("debezium/dbz#1554")
    void whenSameLsnEventsSpanMultipleResumeCallsSkipCountShouldDecrementCorrectly() {
        // Verify that skipMessage skip counter decrements to exactly zero and then
        // all subsequent messages at the same LSN pass through.
        final Lsn lastCommitStoredLsn = Lsn.valueOf(100L);
        final Lsn lastEventStoredLsn = Lsn.valueOf(140L);
        final long processed = 4;
        final var locator = new WalPositionLocator(lastCommitStoredLsn, lastEventStoredLsn,
                ReplicationMessage.Operation.INSERT, processed);

        final Lsn beginLsn = Lsn.valueOf(110L);
        final Lsn sameLsn = Lsn.valueOf(140L);
        final Lsn commitLsn = Lsn.valueOf(150L);

        final var beginMessage = createBeginMessage(1L);
        final var insertMessage = createInsertMessage(2L);

        // Search phase
        assertThat(locator.resumeFromLsn(beginLsn, beginMessage)).isEmpty();
        for (int i = 0; i < processed; i++) {
            assertThat(locator.resumeFromLsn(sameLsn, insertMessage)).isEmpty();
        }
        // Event after processed count triggers resume
        final Optional<Lsn> result = locator.resumeFromLsn(sameLsn, insertMessage);
        assertThat(result).isPresent();

        // Replay phase: exactly 4 skips, then all pass
        locator.enableFiltering();
        assertThat(locator.skipMessage(beginLsn)).isTrue();
        for (int i = 0; i < processed; i++) {
            assertThat(locator.skipMessage(sameLsn)).isTrue();
        }
        // Remaining events at same LSN should all pass
        assertThat(locator.skipMessage(sameLsn)).isFalse();
        assertThat(locator.skipMessage(sameLsn)).isFalse();
        assertThat(locator.skipMessage(commitLsn)).isFalse();
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
