/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures;
import io.debezium.doc.FixFor;

/**
 * The {@link CachedEvent} view of the stored event: only the undo marker reports a rollback, the row's
 * sequence is copied as mined, and the packed row id decodes back to its text.
 *
 * @author Chris Cranford
 */
public class CachedEventTest {

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldReportRollbackOnlyForUndoMarker() {
        for (LogMinerRedoEvent event : LogMinerRedoEventFixtures.allEvents()) {
            assertThat(event.isRollback())
                    .as(event.getEventType().name())
                    .isEqualTo(event.equals(LogMinerRedoEventFixtures.rollbackMarker()));
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldCopyTransactionSequenceAsMined() {
        assertThat(LogMinerRedoEventFixtures.insert().getTransactionSequence()).isZero();
        assertThat(LogMinerRedoEventFixtures.internalEvent().getTransactionSequence()).isEqualTo(1L);
        assertThat(LogMinerRedoEventFixtures.xmlBegin().getTransactionSequence()).isEqualTo(2L);
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldDecodeRowIdThroughTheInterface() {
        final CachedEvent event = LogMinerRedoEventFixtures.insert();
        assertThat(event.getRowIdAsString()).isEqualTo(LogMinerRedoEventFixtures.ROW_ID);
    }
}
