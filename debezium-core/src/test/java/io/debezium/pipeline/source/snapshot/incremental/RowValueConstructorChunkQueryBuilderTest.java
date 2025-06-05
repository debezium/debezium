/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

@RunWith(MockitoJUnitRunner.class)
public class RowValueConstructorChunkQueryBuilderTest {

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

    protected static class NullHandlingJdbcConnection extends JdbcConnection {
        private final boolean nullsLast;

        public NullHandlingJdbcConnection(JdbcConfiguration config, ConnectionFactory connectionFactory,
                                          String openingQuoteCharacter, String closingQuoteCharacter,
                                          boolean nullsLast) {
            super(config, connectionFactory, openingQuoteCharacter, closingQuoteCharacter);
            this.nullsLast = nullsLast;
        }

        @Override
        public Optional<Boolean> nullsSortLast() {
            return Optional.of(nullsLast);
        }
    }

    @Test
    public void testBuildQueryOnePkColumn() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT \"pk1\", \"val1\", \"val2\" FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT \"pk1\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"pk1\" > ? AND \"pk1\" <= ? ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryOnePkColumnWithAdditionalCondition() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT \"pk1\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT \"pk1\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"pk1\" > ? AND \"pk1\" <= ? AND \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryTwoPkColumnsWithAdditionalConditionWithSurrogateKey() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column pk2 = Column.editor().name("pk2").optional(false).create();
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
                .isEqualTo("SELECT \"pk1\", \"pk2\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT \"pk1\", \"pk2\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"pk2\" > ? AND \"pk2\" <= ? AND \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumns() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column pk2 = Column.editor().name("pk2").optional(false).create();
        final Column pk3 = Column.editor().name("pk3").optional(false).create();
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
                "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\", \"val2\" FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE ROW(\"pk1\", \"pk2\", \"pk3\") > ROW(?, ?, ?) AND ROW(\"pk1\", \"pk2\", \"pk3\") <= ROW(?, ?, ?) ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumnsWithAdditionalCondition() {
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column pk2 = Column.editor().name("pk2").optional(false).create();
        final Column pk3 = Column.editor().name("pk3").optional(false).create();
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
                .isEqualTo(
                        "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\", \"val2\" FROM \"s1\".\"table1\" WHERE ROW(\"pk1\", \"pk2\", \"pk3\") > ROW(?, ?, ?) AND ROW(\"pk1\", \"pk2\", \"pk3\") <= ROW(?, ?, ?) AND \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryOptionalThrowsUnsupported() {
        final RelationalDatabaseConnectorConfig config = buildConfig(config().getJdbcConfig().edit()
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.table1:pk1").build());
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config, new JdbcConnection(config.getJdbcConfig(), c -> null, "\"", "\""));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final Column val1 = Column.editor().name("val1").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .create();

        assertThatThrownBy(() -> chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The sort order of NULL values in the incremental snapshot key is unknown.");
    }

    @Test
    public void testBuildQueryOptionalKeysNulls() {
        // Here we just prove correct fallback behavior to the base class when NULL values may be present.
        // More detailed tests are in DefaultChunkQueryBuilderTest.
        final RelationalDatabaseConnectorConfig config = buildConfig(config().getJdbcConfig().edit()
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.table1:pk1,pk2,pk3").build());
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config, new NullHandlingJdbcConnection(config.getJdbcConfig(), c -> null, "\"", "\"", false));
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final Column pk2 = Column.editor().name("pk2").optional(true).create();
        final Column pk3 = Column.editor().name("pk3").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(pk3)
                .addColumn(val1)
                .create();

        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\" FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");

        // Next chunk position and maximum key are both completely NOT NULL
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                // chunk end
                "SELECT \"pk1\", \"pk2\", \"pk3\", \"val1\" FROM \"s1\".\"table1\" WHERE " +
                        "(((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) " +
                        // last table row
                        "AND NOT (((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }
}
