/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class BinlogSnapshotChangeEventSourceTest {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldReleaseTablesOnceSnapshotCompletes() {
        final BinlogSnapshotChangeEventSource source = mock(BinlogSnapshotChangeEventSource.class, CALLS_REAL_METHODS);
        final RelationalSnapshotContext ctx = (RelationalSnapshotContext) source.prepare(mock(BinlogPartition.class), false);
        final TableId id = new TableId("db", null, "t");
        ctx.tables.overwriteTable(Table.editor().tableId(id).create());
        ctx.capturedTables = Set.of(id);
        ctx.capturedSchemaTables = Set.of(id);

        source.completed(ctx);

        assertThat(ctx.tables.size()).isZero();
        assertThat(ctx.capturedTables).isEmpty();
        assertThat(ctx.capturedSchemaTables).isEmpty();
    }
}
