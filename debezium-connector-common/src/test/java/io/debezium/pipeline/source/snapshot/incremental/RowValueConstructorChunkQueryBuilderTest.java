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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

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

@ExtendWith(MockitoExtension.class)
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
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE \"pk1\" > ? AND \"pk1\" <= ? ORDER BY \"pk1\" LIMIT 1024");
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
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE \"pk1\" > ? AND \"pk1\" <= ? AND \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
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
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1 });
        context.maximumKey(new Object[]{ 10 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE \"pk2\" > ? AND \"pk2\" <= ? AND \"val1\"=foo ORDER BY \"pk2\" LIMIT 1024");
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
                "SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ROW(\"pk1\", \"pk2\", \"pk3\") > ROW(?, ?, ?) AND ROW(\"pk1\", \"pk2\", \"pk3\") <= ROW(?, ?, ?) ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
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
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ROW(\"pk1\", \"pk2\", \"pk3\") > ROW(?, ?, ?) AND ROW(\"pk1\", \"pk2\", \"pk3\") <= ROW(?, ?, ?) AND \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
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
                "SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");

        // Next chunk position and maximum key are both completely NOT NULL
        context.nextChunkPosition(new Object[]{ 1, 5, 3 });
        context.maximumKey(new Object[]{ 10, 50, 30 });
        assertThat(chunkQueryBuilder.buildChunkQuery(context, table, Optional.empty())).isEqualTo(
                // chunk end
                "SELECT * FROM \"s1\".\"table1\" WHERE " +
                        "(((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) " +
                        // last table row
                        "AND NOT (((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                        "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    // Builds a lower/upper bound condition in isolation so both the inclusive and exclusive forms can be asserted directly.
    private static String addBound(ChunkQueryBuilder<TableId> builder, boolean upper, List<Column> pkColumns, Object[] boundaryKey, boolean inclusiveFinal) {
        final StringBuilder sql = new StringBuilder();
        if (upper) {
            builder.addUpperBound(pkColumns, boundaryKey, sql, inclusiveFinal);
        }
        else {
            builder.addLowerBound(pkColumns, boundaryKey, sql, inclusiveFinal);
        }
        return sql.toString();
    }

    @Test
    public void testAddBoundsSingleNullableColumnWithoutNullValues() {
        // A single nullable key column forces the builder to fall back to the NULL-aware base class implementation
        // (rather than ROW() syntax), even though the boundary value itself is not NULL.
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new NullHandlingJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"", false));
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final List<Column> pkColumns = List.of(pk1);
        final Object[] boundary = new Object[]{ 5 };

        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, false))
                .isEqualTo("((\"pk1\" > ? AND \"pk1\" IS NOT NULL))");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, true))
                .isEqualTo("((\"pk1\" >= ? AND \"pk1\" IS NOT NULL))");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, false))
                .isEqualTo("NOT ((\"pk1\" >= ? AND \"pk1\" IS NOT NULL))");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, true))
                .isEqualTo("NOT ((\"pk1\" > ? AND \"pk1\" IS NOT NULL))");
    }

    @Test
    public void testAddBoundsSingleNullableColumnWithNullValues() {
        // A single nullable key column whose boundary value is NULL: the base class produces IS NULL / literal
        // conditions depending on the sort order and inclusiveness of the bound.
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new NullHandlingJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"", false));
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final List<Column> pkColumns = List.of(pk1);
        final Object[] boundary = new Object[]{ null };

        // NULL sorts first: nothing is strictly greater than NULL except non-NULL values, and everything is >= NULL.
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, false))
                .isEqualTo("(\"pk1\" IS NOT NULL)");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, true))
                .isEqualTo("(1 = 1)");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, false))
                .isEqualTo("NOT (1 = 1)");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, true))
                .isEqualTo("NOT (\"pk1\" IS NOT NULL)");
    }

    @Test
    public void testAddBoundsMultipleNullableColumnsWithoutNullValues() {
        // Multiple nullable key columns force the fallback to the NULL-aware base class implementation. With no NULL
        // boundary values, every column comparison is guarded by an IS NOT NULL clause (NULL sorts first).
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new NullHandlingJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"", false));
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final Column pk2 = Column.editor().name("pk2").optional(true).create();
        final Column pk3 = Column.editor().name("pk3").optional(true).create();
        final List<Column> pkColumns = List.of(pk1, pk2, pk3);
        final Object[] boundary = new Object[]{ 1, 5, 3 };

        final String lowerExclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND (\"pk3\" > ? AND \"pk3\" IS NOT NULL))";
        final String lowerInclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND (\"pk3\" >= ? AND \"pk3\" IS NOT NULL))";

        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, false)).isEqualTo("(" + lowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, boundary, true)).isEqualTo("(" + lowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, false)).isEqualTo("NOT (" + lowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, boundary, true)).isEqualTo("NOT (" + lowerExclusive + ")");
    }

    @Test
    public void testAddBoundsMultipleNullableColumnsWithNullValues() {
        // Multiple nullable key columns with NULL boundary values. Equality comparisons against a NULL value become
        // IS NULL, and the greater-than comparison on a NULL column collapses per the NULL-sorts-first rules.
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new NullHandlingJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"", false));
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final Column pk2 = Column.editor().name("pk2").optional(true).create();
        final Column pk3 = Column.editor().name("pk3").optional(true).create();
        final List<Column> pkColumns = List.of(pk1, pk2, pk3);

        // NULL in a middle column (pk2).
        final Object[] middleNull = new Object[]{ 1, null, 3 };
        final String middleLowerExclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NOT NULL) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NULL AND (\"pk3\" > ? AND \"pk3\" IS NOT NULL))";
        final String middleLowerInclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NOT NULL) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NULL AND (\"pk3\" >= ? AND \"pk3\" IS NOT NULL))";
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, middleNull, false)).isEqualTo("(" + middleLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, middleNull, true)).isEqualTo("(" + middleLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, middleNull, false)).isEqualTo("NOT (" + middleLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, middleNull, true)).isEqualTo("NOT (" + middleLowerExclusive + ")");

        // NULL in the leading column (pk1).
        final Object[] leadingNull = new Object[]{ null, 50, 30 };
        final String leadingLowerExclusive = "(\"pk1\" IS NOT NULL) " +
                "OR (\"pk1\" IS NULL AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" IS NULL AND \"pk2\" = ? AND (\"pk3\" > ? AND \"pk3\" IS NOT NULL))";
        final String leadingLowerInclusive = "(\"pk1\" IS NOT NULL) " +
                "OR (\"pk1\" IS NULL AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" IS NULL AND \"pk2\" = ? AND (\"pk3\" >= ? AND \"pk3\" IS NOT NULL))";
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, leadingNull, false)).isEqualTo("(" + leadingLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, leadingNull, true)).isEqualTo("(" + leadingLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, leadingNull, false)).isEqualTo("NOT (" + leadingLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, leadingNull, true)).isEqualTo("NOT (" + leadingLowerExclusive + ")");

        // NULL in the last column (pk3).
        final Object[] lastNull = new Object[]{ 30, 50, null };
        final String lastLowerExclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" IS NOT NULL)";
        final String lastLowerInclusive = "((\"pk1\" > ? AND \"pk1\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? AND \"pk2\" IS NOT NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND 1 = 1)";
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, lastNull, false)).isEqualTo("(" + lastLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, lastNull, true)).isEqualTo("(" + lastLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, lastNull, false)).isEqualTo("NOT (" + lastLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, lastNull, true)).isEqualTo("NOT (" + lastLowerExclusive + ")");
    }

    @Test
    public void testAddBoundsMultipleNullableColumnsWithNullValuesNullsSortLast() {
        // Multiple nullable key columns with NULL boundary values. Equality comparisons against a NULL value become
        // IS NULL, and the greater-than comparison on a NULL column collapses per the NULL-sorts-first rules.
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = new RowValueConstructorChunkQueryBuilder<>(
                config(), new NullHandlingJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"", true));
        final Column pk1 = Column.editor().name("pk1").optional(true).create();
        final Column pk2 = Column.editor().name("pk2").optional(true).create();
        final Column pk3 = Column.editor().name("pk3").optional(true).create();
        final List<Column> pkColumns = List.of(pk1, pk2, pk3);

        // NULL in a middle column (pk2).
        final Object[] middleNull = new Object[]{ 1, null, 3 };
        final String middleLowerExclusive = "((\"pk1\" > ? OR \"pk1\" IS NULL)) " +
                "OR (\"pk1\" = ? AND 1 = 0) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NULL AND (\"pk3\" > ? OR \"pk3\" IS NULL))";
        final String middleLowerInclusive = "((\"pk1\" > ? OR \"pk1\" IS NULL)) " +
                "OR (\"pk1\" = ? AND 1 = 0) " +
                "OR (\"pk1\" = ? AND \"pk2\" IS NULL AND (\"pk3\" >= ? OR \"pk3\" IS NULL))";
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, middleNull, false)).isEqualTo("(" + middleLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, middleNull, true)).isEqualTo("(" + middleLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, middleNull, false)).isEqualTo("NOT (" + middleLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, middleNull, true)).isEqualTo("NOT (" + middleLowerExclusive + ")");

        // NULL in the leading column (pk1).
        final Object[] leadingNull = new Object[]{ null, 50, 30 };
        final String leadingLowerExclusive = "(1 = 0) " +
                "OR (\"pk1\" IS NULL AND (\"pk2\" > ? OR \"pk2\" IS NULL)) " +
                "OR (\"pk1\" IS NULL AND \"pk2\" = ? AND (\"pk3\" > ? OR \"pk3\" IS NULL))";
        final String leadingLowerInclusive = "(1 = 0) " +
                "OR (\"pk1\" IS NULL AND (\"pk2\" > ? OR \"pk2\" IS NULL)) " +
                "OR (\"pk1\" IS NULL AND \"pk2\" = ? AND (\"pk3\" >= ? OR \"pk3\" IS NULL))";
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, leadingNull, false)).isEqualTo("(" + leadingLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, leadingNull, true)).isEqualTo("(" + leadingLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, leadingNull, false)).isEqualTo("NOT (" + leadingLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, leadingNull, true)).isEqualTo("NOT (" + leadingLowerExclusive + ")");

        // NULL in the last column (pk3).
        final Object[] lastNull = new Object[]{ 30, 50, null };
        final String lastLowerExclusive = "((\"pk1\" > ? OR \"pk1\" IS NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? OR \"pk2\" IS NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND 1 = 0)";
        final String lastLowerInclusive = "((\"pk1\" > ? OR \"pk1\" IS NULL)) " +
                "OR (\"pk1\" = ? AND (\"pk2\" > ? OR \"pk2\" IS NULL)) " +
                "OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" IS NULL)";

        assertThat(addBound(chunkQueryBuilder, false, pkColumns, lastNull, false)).isEqualTo("(" + lastLowerExclusive + ")");
        assertThat(addBound(chunkQueryBuilder, false, pkColumns, lastNull, true)).isEqualTo("(" + lastLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, lastNull, false)).isEqualTo("NOT (" + lastLowerInclusive + ")");
        assertThat(addBound(chunkQueryBuilder, true, pkColumns, lastNull, true)).isEqualTo("NOT (" + lastLowerExclusive + ")");
    }
}
