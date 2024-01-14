/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

@RunWith(MockitoJUnitRunner.class)
public class DefaultChunkQueryBuilderTest {

    protected RelationalDatabaseConnectorConfig config() {
        return buildConfig(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core").build());
    }

    protected RelationalDatabaseConnectorConfig buildConfig(Configuration configuration) {
        return new RelationalDatabaseConnectorConfig(configuration, null, null,
                0, ColumnFilterMode.CATALOG, true) {
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
        };
    }

    @Test
    public void testBuildQueryOnePkColumn() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) AND NOT (\"pk1\" > ?) ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryOnePkColumnWithAdditionalCondition() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) AND NOT (\"pk1\" > ?) AND \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryTwoPkColumnsWithAdditionalConditionWithSurrogateKey() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2").create();
        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "pk2");
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE (\"pk2\" > ?) AND NOT (\"pk2\" > ?) AND \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumns() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column pk3 = Column.editor().name("pk3").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(pk3)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2", "pk3").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumnsWithAdditionalCondition() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column pk3 = Column.editor().name("pk3").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(pk3)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2", "pk3").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryTwoPkColumnsWithSurrogateKey() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2").create();
        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "pk2");
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty()))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk2\" LIMIT 1024");
    }

    @Test
    public void testMaxQuery() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1")).addColumn(pk1).addColumn(pk2)
                .addColumn(val1).addColumn(val2).setPrimaryKeyNames("pk1", "pk2").create();
        assertThat(chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, table, Optional.empty()))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" DESC, \"pk2\" DESC LIMIT 1");
    }

    @Test
    public void testMaxQueryWithAdditionalCondition() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1")).addColumn(pk1).addColumn(pk2)
                .addColumn(val1).addColumn(val2).setPrimaryKeyNames("pk1", "pk2").create();
        assertThat(chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" DESC, \"pk2\" DESC LIMIT 1");
    }

    @Test
    public void testMaxQueryWithSurrogateKey() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1")).addColumn(pk1).addColumn(pk2)
                .addColumn(val1).addColumn(val2).setPrimaryKeyNames("pk1", "pk2").create();
        context.addDataCollectionNamesToSnapshot("12345", List.of(table.id().toString()), List.of(), "pk2");
        assertThat(chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, table, Optional.empty()))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk2\" DESC LIMIT 1");
    }

    @Test
    @FixFor("DBZ-5727")
    public void testBuildProjectionWithColumnIncludeList() {
        final RelationalDatabaseConnectorConfig config = buildConfig(config().getJdbcConfig().edit()
                .with(RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST, ".*\\.(pk1|pk2|val1|val2)$").build());
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config, new JdbcConnection(config.getJdbcConfig(), c -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        String actualProjection = chunkQueryBuilder.buildChunkQuery(context, createTwoPrimaryKeysTable(), Optional.empty());
        String expectedProjection = "SELECT \"pk1\", \"pk2\", \"val1\", \"val2\" FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\" LIMIT 1024";
        assertThat(actualProjection).isEqualTo(expectedProjection);
    }

    @Test
    @FixFor("DBZ-5727")
    public void testBuildProjectionWithColumnExcludeList() {
        final RelationalDatabaseConnectorConfig config = buildConfig(config().getJdbcConfig().edit()
                .with(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST, ".*\\.(pk2|val3)$").build());
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new DefaultChunkQueryBuilder<>(
                config, new JdbcConnection(config.getJdbcConfig(), c -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        String actualProjection = chunkQueryBuilder.buildChunkQuery(context, createTwoPrimaryKeysTable(), Optional.empty());
        String expectedProjection = "SELECT \"pk1\", \"val1\", \"val2\" FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\" LIMIT 1024";
        assertThat(actualProjection).isEqualTo(expectedProjection);
    }

    private Table createTwoPrimaryKeysTable() {
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Column val3 = Column.editor().name("val3").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1).addColumn(pk2).addColumn(val1).addColumn(val2).addColumn(val3)
                .setPrimaryKeyNames("pk1", "pk2").create();
        return table;
    }
}
