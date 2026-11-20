/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link TransactionEventJournal}.
 *
 * @author Chris Cranford
 */
public class TransactionEventJournalTest {

    @Test
    @FixFor("debezium/dbz#3032")
    public void testEmptyJournalHasNoLastEventOrFirstEvent() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        assertThat(journal.getLastEventScn().isNull()).isTrue();
        assertThat(journal.getFirstEventId()).isEqualTo(-1);
        assertThat(journal.isRewindable(Scn.valueOf(100))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testEventsAtOrBeforeReadPositionAreDiscarded() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(101), false);
        journal.eventAdded(1, Scn.valueOf(105), false);
        journal.eventAdded(2, Scn.valueOf(110), false);

        journal.discardEventsUpTo(Scn.valueOf(105));
        assertThat(journal.getFirstEventId()).isEqualTo(2);
        assertThat(journal.getLastEventScn()).isEqualTo(Scn.valueOf(110));
        assertThat(journal.isRewindable(Scn.valueOf(105))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testLastEventScnIsRetainedAfterEventsAreDiscarded() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(101), false);

        journal.discardEventsUpTo(Scn.valueOf(150));
        assertThat(journal.getFirstEventId()).isEqualTo(-1);
        assertThat(journal.getLastEventScn()).isEqualTo(Scn.valueOf(101));
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testNotRewindableAfterRollbackToSavepointAfterReadPosition() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(101), false);
        journal.eventAdded(1, Scn.valueOf(120), true);

        journal.discardEventsUpTo(Scn.valueOf(110));
        assertThat(journal.isRewindable(Scn.valueOf(110))).isFalse();

        journal.discardEventsUpTo(Scn.valueOf(120));
        assertThat(journal.isRewindable(Scn.valueOf(120))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testNotRewindableWhenEventAtOrBeforeReadPositionFollowsLaterEvent() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(120), false);
        journal.eventAdded(1, Scn.valueOf(105), false);

        journal.discardEventsUpTo(Scn.valueOf(110));
        assertThat(journal.isRewindable(Scn.valueOf(110))).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testNotRewindableBeforeDiscardedEvents() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(120), false);

        // The read position was advanced past the event, so it cannot be rewound to a read position before it
        journal.discardEventsUpTo(Scn.valueOf(200));
        assertThat(journal.isRewindable(Scn.valueOf(119))).isFalse();
        assertThat(journal.isRewindable(Scn.valueOf(200))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testClearEventsKeepsLastEventScn() {
        final TransactionEventJournal journal = new TransactionEventJournal();
        journal.eventAdded(0, Scn.valueOf(101), false);
        journal.eventAdded(1, Scn.valueOf(120), false);

        journal.clearEvents();
        assertThat(journal.getFirstEventId()).isEqualTo(-1);
        assertThat(journal.getLastEventScn()).isEqualTo(Scn.valueOf(120));
    }
}
