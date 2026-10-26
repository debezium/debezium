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
 * The stored event and the row view rebuilt from it carry the same columns, so a stored event parses at commit
 * exactly as its row would have while mining.
 *
 * @author Chris Cranford
 */
public class LogMinerRedoEventTest {

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldRebuildRowViewWithTheStoredColumns() {
        final LogMinerRedoEvent event = LogMinerRedoEventFixtures.updateWithErrorStatus();

        final LogMinerEventRow row = LogMinerEventRow.fromRedoEvent(event, "0a000b0000c40000");

        assertThat(row.getScn()).isEqualTo(event.getScn());
        assertThat(row.getTableId()).isEqualTo(event.getTableId());
        assertThat(row.getTableName()).isEqualTo(event.getTableId().table());
        assertThat(row.getTablespaceName()).isEqualTo(event.getTableId().schema());
        assertThat(row.getEventType()).isEqualTo(event.getEventType());
        assertThat(row.getChangeTime()).isEqualTo(event.getChangeTime());
        assertThat(row.getTransactionId()).isEqualTo("0a000b0000c40000");
        assertThat(row.getRowId()).isEqualTo(event.getRowIdAsString());
        assertThat(row.isRollbackFlag()).isEqualTo(event.isRollback());
        assertThat(row.getRsId()).isEqualTo(event.getRsId());
        assertThat(row.getRedoSql()).isEqualTo(event.getRedoSql());
        assertThat(row.getStatus()).isEqualTo(event.getStatus());
        assertThat(row.hasErrorStatus()).isTrue();
        assertThat(row.getInfo()).isEqualTo(event.getInfo());
        assertThat(row.getObjectId()).isEqualTo(event.getObjectId());
        assertThat(row.getDataObjectId()).isEqualTo(event.getDataObjectId());
        assertThat(row.getTransactionSequence()).isEqualTo(event.getTransactionSequence());

        // Storing the row view again yields the same event
        assertThat(new LogMinerRedoEvent(row)).isEqualTo(event);
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldOmitRedoSqlWhenAskedTo() {
        final LogMinerEventRow row = LogMinerEventRow.fromRedoEvent(LogMinerRedoEventFixtures.insert(), "1");

        final LogMinerRedoEvent stored = new LogMinerRedoEvent(row, false);

        assertThat(stored.getRedoSql()).isNull();
        assertThat(stored.getEventType()).isEqualTo(EventType.INSERT);
        assertThat(stored.getTableId()).isEqualTo(row.getTableId());
    }
}
