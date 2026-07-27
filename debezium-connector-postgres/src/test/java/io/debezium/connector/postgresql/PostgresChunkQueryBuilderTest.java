/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link PostgresChunkQueryBuilder} side-map interaction (DBZ-2020).
 */
public class PostgresChunkQueryBuilderTest {

    @Mock
    private PostgresSchema schema;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    private RelationalDatabaseConnectorConfig config() {
        return new RelationalDatabaseConnectorConfig(
                Configuration.create()
                        .with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                        .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
                        .build(),
                null, null, 0, ColumnFilterMode.SCHEMA, true) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }

    @Test
    @FixFor("DBZ-2020")
    public void shouldKeepSelectStarWhenSideMapIsEmpty() {
        final TableId tableId = new TableId(null, "s1", "table1");
        when(schema.getGeneratedColumnsForTableId(tableId)).thenReturn(Collections.emptyList());

        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new PostgresChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\""), schema);
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Table table = Table.editor().tableId(tableId)
                .addColumn(pk1)
                .addColumn(val1)
                .setPrimaryKeyNames("pk1")
                .create();

        assertThat(chunkQueryBuilder.buildChunkQuery(new SignalBasedIncrementalSnapshotContext<>(), table, Optional.empty()))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    @FixFor("DBZ-2020")
    public void shouldExpandProjectionWhenSideMapHasGeneratedColumnsAfterPrune() {
        // Simulates pgoutput refreshFromIncrementalSnapshot: Table no longer lists gen1, but the
        // side map still knows about it so buildProjection must expand instead of SELECT *.
        final TableId tableId = new TableId(null, "s1", "table1");
        when(schema.getGeneratedColumnsForTableId(tableId)).thenReturn(List.of("gen1"));

        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new PostgresChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\""), schema);
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Table prunedTable = Table.editor().tableId(tableId)
                .addColumn(pk1)
                .addColumn(val1)
                .setPrimaryKeyNames("pk1")
                .create();

        assertThat(chunkQueryBuilder.buildChunkQuery(new SignalBasedIncrementalSnapshotContext<>(), prunedTable, Optional.empty()))
                .isEqualTo("SELECT \"pk1\", \"val1\" FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    @FixFor("DBZ-2020")
    public void shouldOmitSideMapGeneratedColumnStillPresentOnTable() {
        // Side map is the source of truth: even if the in-memory column is not marked generated,
        // a name present in the side map must be dropped from the explicit projection.
        final TableId tableId = new TableId(null, "s1", "table1");
        when(schema.getGeneratedColumnsForTableId(tableId)).thenReturn(List.of("gen1"));

        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new PostgresChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\""), schema);
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column gen1 = Column.editor().name("gen1").create();
        final Table table = Table.editor().tableId(tableId)
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(gen1)
                .setPrimaryKeyNames("pk1")
                .create();

        assertThat(chunkQueryBuilder.buildChunkQuery(new SignalBasedIncrementalSnapshotContext<>(), table, Optional.empty()))
                .isEqualTo("SELECT \"pk1\", \"val1\" FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
    }
}
