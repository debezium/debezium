/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.RowValueConstructorChunkQueryBuilder;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Unit tests for {@link SnapshotChunkQueryBuilder#buildChunkQuery}.
 * <p>
 * A chunk with no boundaries returns the base select unchanged. A first chunk carries only an upper-bound
 * condition, middle chunks carry lower + (exclusive) upper bounds, and the last chunk carries an inclusive
 * upper bound. The condition shape depends on the {@link ChunkQueryBuilder} of the underlying
 * {@link JdbcConnection}: ANSI OR-expanded tuples for the default builder vs {@code ROW()} syntax for the
 * row value constructor builder used by e.g. PostgreSQL.
 */
@ExtendWith(MockitoExtension.class)
public class SnapshotChunkQueryBuilderTest {

    private static final String BASE_SELECT = "SELECT * FROM \"s1\".\"table1\"";

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

    /**
     * A {@link JdbcConnection} whose {@link #chunkQueryBuilder(RelationalDatabaseConnectorConfig)} returns a
     * {@link RowValueConstructorChunkQueryBuilder}, mimicking the PostgreSQL connection.
     */
    protected static class RowValueConstructorJdbcConnection extends JdbcConnection {
        public RowValueConstructorJdbcConnection(JdbcConfiguration config, ConnectionFactory connectionFactory,
                                                 String openingQuoteCharacter, String closingQuoteCharacter) {
            super(config, connectionFactory, openingQuoteCharacter, closingQuoteCharacter);
        }

        @Override
        public <T extends DataCollectionId> ChunkQueryBuilder<T> chunkQueryBuilder(RelationalDatabaseConnectorConfig connectorConfig) {
            return new RowValueConstructorChunkQueryBuilder<>(connectorConfig, this);
        }
    }

    private JdbcConnection defaultConnection() {
        return new JdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"");
    }

    private JdbcConnection rowValueConstructorConnection() {
        return new RowValueConstructorJdbcConnection(config().getJdbcConfig(), c -> null, "\"", "\"");
    }

    private Table singlePkTable() {
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        return Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
    }

    private Table threePkTable() {
        final Column pk1 = Column.editor().name("pk1").optional(false).create();
        final Column pk2 = Column.editor().name("pk2").optional(false).create();
        final Column pk3 = Column.editor().name("pk3").optional(false).create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        return Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(pk2)
                .addColumn(pk3)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1", "pk2", "pk3").create();
    }

    private SnapshotChunk chunk(Table table, Object[] lowerBounds, Object[] upperBounds, int chunkIndex, int totalChunks) {
        return new SnapshotChunk(table.id(), table, lowerBounds, upperBounds, chunkIndex, totalChunks,
                1, 1, BASE_SELECT, OptionalLong.empty());
    }

    @Test
    public void singleChunkReturnsBaseSelectUnchanged() {
        final Table table = singlePkTable();
        final List<Column> keyColumns = table.primaryKeyColumns();
        // The only chunk has neither lower nor upper bound, so no WHERE/ORDER BY is added.
        final SnapshotChunk singleChunk = chunk(table, null, null, 0, 1);

        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config()).buildChunkQuery(singleChunk, keyColumns, BASE_SELECT))
                .isEqualTo(BASE_SELECT);
        assertThat(new SnapshotChunkQueryBuilder(rowValueConstructorConnection(), config()).buildChunkQuery(singleChunk, keyColumns, BASE_SELECT))
                .isEqualTo(BASE_SELECT);
    }

    @Test
    public void firstChunkSinglePkDefaultBuilderHasUpperBoundOnly() {
        final Table table = singlePkTable();
        // First chunk of three: no lower bound, exclusive upper bound.
        final SnapshotChunk firstChunk = chunk(table, null, new Object[]{ 10 }, 0, 3);
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config()).buildChunkQuery(firstChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE NOT ((\"pk1\" > ?) OR (\"pk1\" = ?)) ORDER BY \"pk1\"");
    }

    @Test
    public void middleChunkSinglePkDefaultBuilder() {
        final Table table = singlePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 1, 3);
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config()).buildChunkQuery(middleChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ?)) ORDER BY \"pk1\"");
    }

    @Test
    public void lastChunkSinglePkDefaultBuilderHasInclusiveUpperBound() {
        final Table table = singlePkTable();
        // Last chunk of three -> upper bound is inclusive.
        final SnapshotChunk lastChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 2, 3);
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config()).buildChunkQuery(lastChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ?)) AND NOT (\"pk1\" > ?) ORDER BY \"pk1\"");
    }

    @Test
    public void middleChunkSinglePkRowValueConstructorBuilder() {
        final Table table = singlePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 1, 3);
        assertThat(new SnapshotChunkQueryBuilder(rowValueConstructorConnection(), config()).buildChunkQuery(middleChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"pk1\" >= ? AND \"pk1\" < ? ORDER BY \"pk1\"");
    }

    @Test
    public void lastChunkSinglePkRowValueConstructorBuilderHasInclusiveUpperBound() {
        final Table table = singlePkTable();
        final SnapshotChunk lastChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 2, 3);
        assertThat(new SnapshotChunkQueryBuilder(rowValueConstructorConnection(), config()).buildChunkQuery(lastChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"pk1\" >= ? AND \"pk1\" <= ? ORDER BY \"pk1\"");
    }

    @Test
    public void middleChunkThreePkDefaultBuilder() {
        final Table table = threePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1, 5, 3 }, new Object[]{ 10, 50, 30 }, 1, 3);
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config()).buildChunkQuery(middleChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE " +
                        "((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" = ?)) " +
                        "AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" = ?)) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\"");
    }

    @Test
    public void middleChunkThreePkRowValueConstructorBuilder() {
        final Table table = threePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1, 5, 3 }, new Object[]{ 10, 50, 30 }, 1, 3);
        assertThat(new SnapshotChunkQueryBuilder(rowValueConstructorConnection(), config()).buildChunkQuery(middleChunk, table.primaryKeyColumns(), BASE_SELECT))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE " +
                        "ROW(\"pk1\", \"pk2\", \"pk3\") >= ROW(?, ?, ?) AND ROW(\"pk1\", \"pk2\", \"pk3\") < ROW(?, ?, ?) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\"");
    }

    @Test
    public void baseSelectWithExistingWhereClauseIsPreserved() {
        final Table table = singlePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 1, 3);
        // The boundary condition is wrapped in parentheses and AND-ed with the existing WHERE clause.
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config())
                .buildChunkQuery(middleChunk, table.primaryKeyColumns(), "SELECT * FROM \"s1\".\"table1\" WHERE \"val1\" = 5"))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE (((\"pk1\" > ?) OR (\"pk1\" = ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ?))) AND \"val1\" = 5 ORDER BY \"pk1\"");
    }

    @Test
    public void baseSelectWithExistingOrderByIsReplaced() {
        final Table table = singlePkTable();
        final SnapshotChunk middleChunk = chunk(table, new Object[]{ 1 }, new Object[]{ 10 }, 1, 3);
        // The existing ORDER BY is replaced by an ORDER BY over the key columns.
        assertThat(new SnapshotChunkQueryBuilder(defaultConnection(), config())
                .buildChunkQuery(middleChunk, table.primaryKeyColumns(), "SELECT * FROM \"s1\".\"table1\" ORDER BY \"x\""))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ?)) ORDER BY \"pk1\"");
    }
}
