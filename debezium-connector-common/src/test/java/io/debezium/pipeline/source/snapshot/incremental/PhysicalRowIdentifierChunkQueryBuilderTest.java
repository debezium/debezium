/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class PhysicalRowIdentifierChunkQueryBuilderTest extends DefaultChunkQueryBuilderTest {

    @Test
    public void testPhysicalIdentifierChunkingUsesPrimaryKeyOrdering() {
        final InspectablePhysicalRowIdentifierChunkQueryBuilder chunkQueryBuilder = new InspectablePhysicalRowIdentifierChunkQueryBuilder(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Table table = createTwoPrimaryKeysTable();

        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "ROWID");
        final Table preparedTable = chunkQueryBuilder.prepareTable(context, table);

        assertThat(chunkQueryBuilder.getQueryColumns(context, preparedTable)).extracting(Column::name)
                .containsExactly("ROWID");
        assertThat(chunkQueryBuilder.orderByColumns(context, preparedTable)).extracting(Column::name)
                .containsExactly("pk1", "pk2");
    }

    @Test
    public void testPrefetchQueryOrdersByPhysicalIdentifierExpression() {
        final InspectablePhysicalRowIdentifierChunkQueryBuilder chunkQueryBuilder = new InspectablePhysicalRowIdentifierChunkQueryBuilder(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Table table = createTwoPrimaryKeysTable();

        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "ROWID");
        final Table preparedTable = chunkQueryBuilder.prepareTable(context, table);
        context.nextChunkPosition(new Object[]{ "AAABBB" });

        assertThat(chunkQueryBuilder.prefetchQuery(context, preparedTable, 128, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT ROWID FROM \"s1\".\"table1\" WHERE (\"ROWID\" > ?) "
                        + "AND \"val1\"=foo ORDER BY ROWID OFFSET 127 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    @Test
    public void testResolveChunkEndPositionFallsBackToMaximumKey() {
        final InspectablePhysicalRowIdentifierChunkQueryBuilder chunkQueryBuilder = new InspectablePhysicalRowIdentifierChunkQueryBuilder(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Table table = createTwoPrimaryKeysTable();

        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "ROWID");
        final Table preparedTable = chunkQueryBuilder.prepareTable(context, table);
        context.maximumKey(new Object[]{ "AAACCC" });

        assertThat(chunkQueryBuilder.resolveChunkEndPosition(context, preparedTable, new Object[]{ "AAABBB" }))
                .containsExactly("AAACCC");
    }

    private Table createTwoPrimaryKeysTable() {
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column pk2 = Column.editor().name("pk2").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        return Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2")
                .create();
    }

    private static final class InspectablePhysicalRowIdentifierChunkQueryBuilder extends PhysicalRowIdentifierChunkQueryBuilder<TableId> {

        private InspectablePhysicalRowIdentifierChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                                                  JdbcConnection jdbcConnection) {
            super(config, jdbcConnection, "ROWID", "ROWID", Types.ROWID, Types.ROWID, "ROWID", null, null, false, null);
        }

        private List<Column> orderByColumns(IncrementalSnapshotContext<TableId> context, Table table) {
            return getOrderByColumns(context, table);
        }

        private String prefetchQuery(IncrementalSnapshotContext<TableId> context, Table table, int limit,
                                     Optional<String> additionalCondition) {
            return super.buildPrefetchQuery(context, table, limit, additionalCondition);
        }
    }
}