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
        // When creating WalPositionLocator with default constructor
        WalPositionLocator locator = new WalPositionLocator();

        // Then all stored LSNs should be null
        assertThat(locator.getLastCommitStoredLsn()).isNull();
        assertThat(locator.getLastEventStoredLsn()).isNull();
        assertThat(locator.searchingEnabled()).isFalse();
    }

    @Test
    public void testParameterizedConstructor() {
        // Given stored LSNs and message type
        Lsn lastCommit = Lsn.valueOf(100L);
        Lsn lastEvent = Lsn.valueOf(150L);
        ReplicationMessage.Operation lastType = ReplicationMessage.Operation.COMMIT;

        // When creating WalPositionLocator with parameters
        WalPositionLocator locator = new WalPositionLocator(lastCommit, lastEvent, lastType);

        // Then stored values should be set correctly
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

    // Should this case ever come ?
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
    // resumeFromLsn Tests - No Stored Commit (Default Constructor)
    // =============================================================================

    @Test
    public void testResumeFromLsn_noStoredCommit_returnsFirstLsnReceived() {
        // Given a WalPositionLocator with no stored commit LSN
        WalPositionLocator locator = new WalPositionLocator();
        Lsn lsn = Lsn.valueOf(100L);
        ReplicationMessage message = createBeginMessage(1L);

        // When resumeFromLsn is called
        Optional<Lsn> result = locator.resumeFromLsn(lsn, message);

        // Then it should return the first LSN received
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(lsn);
    }


    // =============================================================================
    // resumeFromLsn Tests - With Stored Commit/Event LSNs
    // =============================================================================

    @Test
    public void testResumeFromLsn_beforeStoredCommit_returnsEmpty() {
        // Given a WalPositionLocator with stored commit LSN
        Lsn storedCommit = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedCommit, ReplicationMessage.Operation.COMMIT);

        // When processing a commit message with LSN before stored commit
        Lsn olderCommitLsn = Lsn.valueOf(50L);
        Optional<Lsn> result = locator.resumeFromLsn(olderCommitLsn, createCommitMessage(1L));

        // Then it should return empty (still searching)
        assertThat(result).isEmpty();
    }

    @Test
    public void testResumeFromLsn_commitAfterStored_withLsnAfterLastEvent_returnsLsnAfterLastEvent() {
        // Given a WalPositionLocator with stored LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedEvent, ReplicationMessage.Operation.COMMIT);

        // When we first match the last event stored LSN, then get a commit after stored commit
        locator.resumeFromLsn(storedEvent, createInsertMessage(1L)); // This sets storeLsnAfterLastEventStoredLsn = true
        Lsn nextLsn = Lsn.valueOf(160L);
        locator.resumeFromLsn(nextLsn, createInsertMessage(2L)); // This sets lsnAfterLastEventStoredLsn

        Lsn newCommitLsn = Lsn.valueOf(200L);
        Optional<Lsn> result = locator.resumeFromLsn(newCommitLsn, createCommitMessage(3L));

        // Then it should return the LSN after the last event stored
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(nextLsn);
    }

    @Test
    public void testResumeFromLsn_commitAfterStored_withTxStartLsn_returnsTxStartLsn() {
        // Given a WalPositionLocator with stored LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedEvent, ReplicationMessage.Operation.COMMIT);

        // When we process a BEGIN message, then a commit after stored commit
        Lsn txStartLsn = Lsn.valueOf(180L);
        locator.resumeFromLsn(txStartLsn, createBeginMessage(1L)); // Sets txStartLsn

        Lsn newCommitLsn = Lsn.valueOf(200L);
        Optional<Lsn> result = locator.resumeFromLsn(newCommitLsn, createCommitMessage(2L));

        // Then it should return the transaction start LSN
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(txStartLsn);
    }

    @Test
    public void testResumeFromLsn_commitAfterStored_noTxStartNoLsnAfter_returnsFirstLsnReceived() {
        // Given a WalPositionLocator with stored LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedEvent, ReplicationMessage.Operation.COMMIT);

        // When we process a commit after stored commit without setting txStartLsn or lsnAfterLastEventStoredLsn
        Lsn firstLsn = Lsn.valueOf(110L);
        locator.resumeFromLsn(firstLsn, createInsertMessage(1L)); // Sets firstLsnReceived

        Lsn newCommitLsn = Lsn.valueOf(200L);
        Optional<Lsn> result = locator.resumeFromLsn(newCommitLsn, createCommitMessage(2L));

        // Then it should return the first LSN received
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(firstLsn);
    }

    @Test
    public void testResumeFromLsn_beginOperation_setsTxStartLsn() {
        // Given a WalPositionLocator with stored LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedCommit, ReplicationMessage.Operation.COMMIT);

        // When processing a BEGIN message
        Lsn beginLsn = Lsn.valueOf(110L);
        Optional<Lsn> result = locator.resumeFromLsn(beginLsn, createBeginMessage(1L));

        // Then it should return empty but set txStartLsn internally
        assertThat(result).isEmpty();

        // Verify by checking subsequent commit uses the txStartLsn
        Lsn commitLsn = Lsn.valueOf(200L);
        Optional<Lsn> commitResult = locator.resumeFromLsn(commitLsn, createCommitMessage(2L));
        assertThat(commitResult).isPresent();
        assertThat(commitResult.get()).isEqualTo(beginLsn);
    }

    @Test
    public void testResumeFromLsn_defaultOperationCase_returnsEmpty() {
        // Given a WalPositionLocator with stored LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedCommit, ReplicationMessage.Operation.COMMIT);

        // When processing a non-BEGIN, non-COMMIT message
        Lsn insertLsn = Lsn.valueOf(110L);
        Optional<Lsn> result = locator.resumeFromLsn(insertLsn, createInsertMessage(1L));

        // Then it should return empty
        assertThat(result).isEmpty();
    }

    // =============================================================================
    // resumeFromLsn Tests - storeLsnAfterLastEventStoredLsn Logic
    // =============================================================================

    @Test
    public void testCompleteWorkflow_searchPhaseToStreamingPhase() {
        // This test demonstrates the complete two-phase workflow
        
        // Given a WalPositionLocator with stored commit/event LSNs (typical restart scenario)
        Lsn storedCommit = Lsn.valueOf(100L);
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedEvent, ReplicationMessage.Operation.COMMIT);

        // PHASE 1: SEARCH PHASE - Find where to restart streaming
        // Multiple resumeFromLsn calls simulate WAL messages being processed to find restart point
        
        Lsn searchLsn1 = Lsn.valueOf(110L);
        Lsn searchLsn2 = Lsn.valueOf(130L);
        Lsn searchLsn3 = Lsn.valueOf(150L); // This matches storedEvent
        Lsn searchLsn4 = Lsn.valueOf(160L); // This should return as restart point
        
        // Process messages during search (all get added to lsnSeen)
        Optional<Lsn> search1 = locator.resumeFromLsn(searchLsn1, createBeginMessage(1L));
        Optional<Lsn> search2 = locator.resumeFromLsn(searchLsn2, createInsertMessage(2L));
        Optional<Lsn> search3 = locator.resumeFromLsn(searchLsn3, createInsertMessage(3L)); // Sets flag
        Optional<Lsn> search4 = locator.resumeFromLsn(searchLsn4, createInsertMessage(4L)); // Returns restart LSN
        
        // Verify search phase results
        assertThat(search1).isEmpty(); // Still searching
        assertThat(search2).isEmpty(); // Still searching
        assertThat(search3).isEmpty(); // Still searching (sets storeLsnAfterLastEventStoredLsn flag)
        assertThat(search4).isPresent(); // Found restart point!
        assertThat(search4.get()).isEqualTo(searchLsn4);
        
        // PHASE 2: STREAMING PHASE - Now start actual WAL streaming
        // Switch to streaming mode
        locator.enableFiltering();
        
        // During streaming, check if LSNs should be skipped
        // LSNs seen during search phase should be skipped (already processed)
        assertThat(locator.skipMessage(searchLsn1)).isTrue(); // Skip - seen during search
        assertThat(locator.skipMessage(searchLsn2)).isTrue(); // Skip - seen during search
        assertThat(locator.skipMessage(searchLsn3)).isTrue(); // Skip - seen during search
        
        // When we reach the restart LSN, filtering gets disabled
        assertThat(locator.skipMessage(searchLsn4)).isFalse(); // Don't skip - this is restart point
        
        // After restart LSN, new streaming LSNs are not skipped
        assertThat(locator.skipMessage(Lsn.valueOf(170L))).isFalse(); // Don't skip - new data
        assertThat(locator.skipMessage(Lsn.valueOf(180L))).isFalse(); // Don't skip - new data
    }

    // =============================================================================
    // resumeFromLsn Tests - storeLsnAfterLastEventStoredLsn Logic
    // =============================================================================

    @Test
    public void testResumeFromLsn_matchingLastEventStored_setsStoreLsnAfterFlag() {
        // Given a WalPositionLocator with stored event LSN
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(Lsn.valueOf(100L), storedEvent, ReplicationMessage.Operation.COMMIT);

        // When processing a message with LSN matching the last event stored
        Optional<Lsn> result = locator.resumeFromLsn(storedEvent, createInsertMessage(1L));

        // Then it should return empty but set the flag for next message
        assertThat(result).isEmpty();

        // Next message should return its LSN as the resume point
        Lsn nextLsn = Lsn.valueOf(160L);
        Optional<Lsn> nextResult = locator.resumeFromLsn(nextLsn, createInsertMessage(2L));
        assertThat(nextResult).isPresent();
        assertThat(nextResult.get()).isEqualTo(nextLsn);
    }

    @Test
    public void testResumeFromLsn_storeLsnAfterFlag_withMatchingLastEventAndTxStart_returnsTxStart() {
        // Given a WalPositionLocator with stored event LSN
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(Lsn.valueOf(100L), storedEvent, ReplicationMessage.Operation.BEGIN);

        // When we have txStartLsn set and storeLsnAfterLastEventStoredLsn is true
        Lsn txStart = Lsn.valueOf(140L);
        locator.resumeFromLsn(txStart, createBeginMessage(1L)); // Sets txStartLsn
        locator.resumeFromLsn(storedEvent, createInsertMessage(2L)); // Sets storeLsnAfterLastEventStoredLsn = true

        // And we get a message with same LSN as lastEventStoredLsn with lastProcessedMessageType = BEGIN
        Optional<Lsn> result = locator.resumeFromLsn(storedEvent, createInsertMessage(3L));

        // Then it should return the transaction start LSN
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(txStart);
    }

    @Test
    public void testResumeFromLsn_storeLsnAfterFlag_withMatchingLastEventAndTxStart_lastProcessedCommit_returnsTxStart() {
        // Given a WalPositionLocator with stored event LSN and lastProcessedMessageType = COMMIT
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(Lsn.valueOf(100L), storedEvent, ReplicationMessage.Operation.COMMIT);

        // When we have txStartLsn set and storeLsnAfterLastEventStoredLsn is true
        Lsn txStart = Lsn.valueOf(140L);
        locator.resumeFromLsn(txStart, createBeginMessage(1L)); // Sets txStartLsn
        locator.resumeFromLsn(storedEvent, createInsertMessage(2L)); // Sets storeLsnAfterLastEventStoredLsn = true

        // And we get a message with same LSN as lastEventStoredLsn
        Optional<Lsn> result = locator.resumeFromLsn(storedEvent, createInsertMessage(3L));

        // Then it should return the transaction start LSN
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(txStart);
    }

    @Test
    public void testResumeFromLsn_storeLsnAfterFlag_withMatchingLastEventAndTxStart_lastProcessedNull_returnsTxStart() {
        // Given a WalPositionLocator with stored event LSN and lastProcessedMessageType = null
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(Lsn.valueOf(100L), storedEvent, null);

        // When we have txStartLsn set and storeLsnAfterLastEventStoredLsn is true
        Lsn txStart = Lsn.valueOf(140L);
        locator.resumeFromLsn(txStart, createBeginMessage(1L)); // Sets txStartLsn
        locator.resumeFromLsn(storedEvent, createInsertMessage(2L)); // Sets storeLsnAfterLastEventStoredLsn = true

        // And we get a message with same LSN as lastEventStoredLsn
        Optional<Lsn> result = locator.resumeFromLsn(storedEvent, createInsertMessage(3L));

        // Then it should return the transaction start LSN
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(txStart);
    }

    @Test
    public void testResumeFromLsn_storeLsnAfterFlag_withMatchingLastEventNoTxStart_returnsEmpty() {
        // Given a WalPositionLocator with stored event LSN
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(Lsn.valueOf(100L), storedEvent, ReplicationMessage.Operation.INSERT);

        // When storeLsnAfterLastEventStoredLsn is true but no txStartLsn
        locator.resumeFromLsn(storedEvent, createInsertMessage(1L)); // Sets storeLsnAfterLastEventStoredLsn = true

        // And we get a message with same LSN as lastEventStoredLsn but lastProcessedMessageType is not BEGIN/COMMIT/null
        Optional<Lsn> result = locator.resumeFromLsn(storedEvent, createInsertMessage(2L));

        // Then it should return empty
        assertThat(result).isEmpty();
    }

    // =============================================================================
    // skipMessage Tests
    // =============================================================================

    @Test
    public void testSkipMessage_passMessagesTrue_returnsFalse() {
        // Given a WalPositionLocator with default state (passMessages = true)
        WalPositionLocator locator = new WalPositionLocator();
        Lsn lsn = Lsn.valueOf(100L);

        // When skipMessage is called
        boolean result = locator.skipMessage(lsn);

        // Then it should return false (don't skip)
        assertThat(result).isFalse();
    }

    @Test
    public void testSkipMessage_filteringEnabled_startStreamingLsnNull_returnsFalseAndEnablesPassMessages() {
        // Given a WalPositionLocator with filtering enabled
        WalPositionLocator locator = new WalPositionLocator();
        locator.enableFiltering(); // Sets passMessages = false
        Lsn lsn = Lsn.valueOf(100L);

        // When skipMessage is called with startStreamingLsn = null
        boolean result = locator.skipMessage(lsn);

        // Then it should return false and enable pass messages
        assertThat(result).isFalse();

        // Subsequent calls should not skip
        boolean secondResult = locator.skipMessage(Lsn.valueOf(200L));
        assertThat(secondResult).isFalse();
    }

    @Test
    public void testSkipMessage_filteringEnabled_matchingStartStreamingLsn_returnsFalseAndEnablesPassMessages() {
        // Given a WalPositionLocator in search phase
        WalPositionLocator locator = new WalPositionLocator();
        Lsn startLsn = Lsn.valueOf(100L);
        
        // PHASE 1: Search phase - find restart LSN (adds to lsnSeen)
        Optional<Lsn> restartLsn = locator.resumeFromLsn(startLsn, createBeginMessage(1L));
        assertThat(restartLsn).isPresent(); // Found restart point
        
        // PHASE 2: Switch to streaming phase
        locator.enableFiltering();

        // When skipMessage is called with the restart LSN during streaming
        boolean result = locator.skipMessage(startLsn);

        // Then it should return false (don't skip) and enable pass messages for future LSNs
        assertThat(result).isFalse();

        // Subsequent calls with new LSNs should not skip (passMessages now true)
        boolean secondResult = locator.skipMessage(Lsn.valueOf(200L));
        assertThat(secondResult).isFalse();
    }

    @Test
    public void testSkipMessage_correctWorkflow_searchThenStream() {
        // Given a WalPositionLocator with stored commit/event LSNs
        Lsn storedCommit = Lsn.valueOf(100L);
        Lsn storedEvent = Lsn.valueOf(150L);
        WalPositionLocator locator = new WalPositionLocator(storedCommit, storedEvent, ReplicationMessage.Operation.COMMIT);

        // PHASE 1: Search phase - multiple resumeFromLsn calls (populates lsnSeen)
        Lsn lsn1 = Lsn.valueOf(110L);
        Lsn lsn2 = Lsn.valueOf(120L);
        Lsn lsn3 = Lsn.valueOf(160L); // This will be after storedEvent
        
        Optional<Lsn> result1 = locator.resumeFromLsn(lsn1, createBeginMessage(1L)); // Empty - still searching
        Optional<Lsn> result2 = locator.resumeFromLsn(lsn2, createInsertMessage(2L)); // Empty - still searching
        locator.resumeFromLsn(storedEvent, createInsertMessage(3L)); // Sets storeLsnAfterLastEventStoredLsn = true
        Optional<Lsn> result3 = locator.resumeFromLsn(lsn3, createInsertMessage(4L)); // Returns restart LSN
        
        assertThat(result1).isEmpty();
        assertThat(result2).isEmpty();
        assertThat(result3).isPresent(); // Found restart point!
        
        // PHASE 2: Switch to streaming phase
        locator.enableFiltering();
        
        // Now during streaming, LSNs seen during search should be skipped
        boolean skip1 = locator.skipMessage(lsn1); // Was seen during search
        boolean skip2 = locator.skipMessage(lsn2); // Was seen during search
        
        assertThat(skip1).isTrue(); // Skip - was processed during search
        assertThat(skip2).isTrue(); // Skip - was processed during search
        
        // When we reach the restart LSN, filtering is disabled
        boolean skipRestart = locator.skipMessage(lsn3);
        assertThat(skipRestart).isFalse(); // Don't skip - this is where streaming starts
        
        // After restart LSN, new LSNs are not skipped
        boolean skipNew = locator.skipMessage(Lsn.valueOf(170L));
        assertThat(skipNew).isFalse(); // Don't skip - new streaming LSN
    }

    @Test
    public void testSkipMessage_filteringEnabled_validLsnNotSeen_throwsException() {
        // Given a WalPositionLocator after search phase
        WalPositionLocator locator = new WalPositionLocator();
        Lsn searchLsn = Lsn.valueOf(100L);
        Lsn unseenLsn = Lsn.valueOf(200L); // Valid LSN not seen during search

        // PHASE 1: Search phase (populates lsnSeen with searchLsn only)
        locator.resumeFromLsn(searchLsn, createBeginMessage(1L));
        
        // PHASE 2: Switch to streaming phase
        locator.enableFiltering();

        // When skipMessage is called with valid but unseen LSN during streaming
        // Then it should throw DebeziumException (this LSN wasn't seen during search)
        assertThatThrownBy(() -> locator.skipMessage(unseenLsn))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Message with LSN")
                .hasMessageContaining("not present among LSNs seen");
    }

    @Test
    public void testSkipMessage_filteringEnabled_invalidLsnNotSeen_returnsTrue() {
        // Given a WalPositionLocator after search phase
        WalPositionLocator locator = new WalPositionLocator();
        Lsn searchLsn = Lsn.valueOf(100L);
        Lsn invalidLsn = Lsn.INVALID_LSN;

        // PHASE 1: Search phase
        locator.resumeFromLsn(searchLsn, createBeginMessage(1L));
        
        // PHASE 2: Switch to streaming phase
        locator.enableFiltering();

        // When skipMessage is called with invalid LSN during streaming
        boolean result = locator.skipMessage(invalidLsn);

        // Then it should return true (skip invalid LSNs)
        assertThat(result).isTrue();
    }

    // =============================================================================
    // enableFiltering Tests
    // =============================================================================

    @Test
    public void testEnableFiltering_setsPassMessagesFalse() {
        // Given a WalPositionLocator with default state
        WalPositionLocator locator = new WalPositionLocator();

        // When enableFiltering is called
        locator.enableFiltering();

        // Then passMessages should be set to false (verified through skipMessage behavior)
        Lsn lsn = Lsn.valueOf(100L);
        // First call should trigger the startStreamingLsn == null condition
        boolean firstResult = locator.skipMessage(lsn);
        assertThat(firstResult).isFalse();
    }

    // =============================================================================
    // searchingEnabled Tests
    // =============================================================================

    @Test
    public void testSearchingEnabled_withNullLastEventStoredLsn_returnsFalse() {
        // Given a WalPositionLocator with null lastEventStoredLsn
        WalPositionLocator locator = new WalPositionLocator();

        // When searchingEnabled is called
        boolean result = locator.searchingEnabled();

        // Then it should return false
        assertThat(result).isFalse();
    }

    @Test
    public void testSearchingEnabled_withNonNullLastEventStoredLsn_returnsTrue() {
        // Given a WalPositionLocator with non-null lastEventStoredLsn
        Lsn lastEvent = Lsn.valueOf(100L);
        WalPositionLocator locator = new WalPositionLocator(null, lastEvent, null);

        // When searchingEnabled is called
        boolean result = locator.searchingEnabled();

        // Then it should return true
        assertThat(result).isTrue();
    }

    // =============================================================================
    // toString Tests
    // =============================================================================

    @Test
    public void testToString_defaultConstructor() {
        // Given a WalPositionLocator with default constructor
        WalPositionLocator locator = new WalPositionLocator();

        // When toString is called
        String result = locator.toString();

        // Then it should contain all field values
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
        // Given a WalPositionLocator with parameters
        Lsn lastCommit = Lsn.valueOf(100L);
        Lsn lastEvent = Lsn.valueOf(150L);
        ReplicationMessage.Operation lastType = ReplicationMessage.Operation.COMMIT;
        WalPositionLocator locator = new WalPositionLocator(lastCommit, lastEvent, lastType);

        // When toString is called
        String result = locator.toString();

        // Then it should contain all field values
        assertThat(result).contains("WalPositionLocator")
                .contains("lastCommitStoredLsn=" + lastCommit)
                .contains("lastEventStoredLsn=" + lastEvent)
                .contains("lastProcessedMessageType=" + lastType);
    }

    @Test
    public void testToString_afterProcessingMessages() {
        // Given a WalPositionLocator after processing some messages
        WalPositionLocator locator = new WalPositionLocator();
        Lsn lsn = Lsn.valueOf(100L);
        locator.resumeFromLsn(lsn, createBeginMessage(1L));

        // When toString is called
        String result = locator.toString();

        // Then it should show updated field values
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
