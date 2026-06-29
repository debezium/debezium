/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link SnapshotMeter}, in particular the {@code TotalTableCount}/{@code RemainingTableCount}
 * bookkeeping that backs the snapshot JMX metrics.
 */
public class SnapshotMeterTest {

    private final SnapshotMeter meter = new SnapshotMeter(Clock.system());

    private static final TableId TABLE_A = TableId.parse("db.schema.a");
    private static final TableId TABLE_B = TableId.parse("db.schema.b");

    @Test
    public void shouldTrackRemainingTablesAsDataSnapshotCompletes() {
        meter.monitoredDataCollectionsDetermined(List.of(TABLE_A, TABLE_B));
        assertThat(meter.getTotalTableCount()).isEqualTo(2);
        assertThat(meter.getRemainingTableCount()).isEqualTo(2);

        meter.dataCollectionSnapshotCompleted(TABLE_A, 10);
        assertThat(meter.getRemainingTableCount()).isEqualTo(1);

        meter.dataCollectionSnapshotCompleted(TABLE_B, 20);
        assertThat(meter.getRemainingTableCount()).isEqualTo(0);
        // The total (captured) count is unaffected by completion.
        assertThat(meter.getTotalTableCount()).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#1479")
    public void shouldReportNoRemainingTablesWhenDataIsNotSnapshotted() {
        // Reproduces the snapshot.mode=no_data path: tables are monitored (so TotalTableCount/CapturedTables
        // are populated), but no data snapshot runs. Marking each captured table completed with zero rows must
        // drain RemainingTableCount to 0, while leaving the captured-table count intact.
        meter.monitoredDataCollectionsDetermined(List.of(TABLE_A, TABLE_B));
        assertThat(meter.getRemainingTableCount()).isEqualTo(2);

        for (TableId tableId : List.of(TABLE_A, TABLE_B)) {
            meter.dataCollectionSnapshotCompleted(tableId, 0);
        }

        assertThat(meter.getRemainingTableCount()).isEqualTo(0);
        assertThat(meter.getTotalTableCount()).isEqualTo(2);
        assertThat(meter.getCapturedTables()).containsExactlyInAnyOrder(TABLE_A.identifier(), TABLE_B.identifier());
    }
}
