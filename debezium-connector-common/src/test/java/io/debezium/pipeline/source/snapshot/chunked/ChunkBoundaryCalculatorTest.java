/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.RowValueConstructorChunkQueryBuilder;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Unit tests for {@link ChunkBoundaryCalculator#buildNextBoundaryQuery}.
 * <p>
 * The first chunk boundary query has no lower-bound condition; every subsequent boundary query includes a
 * lower-bound condition whose shape depends on the {@link ChunkQueryBuilder} provided by the underlying
 * {@link JdbcConnection} (ANSI OR-expanded tuples for the default builder, {@code ROW()} syntax for the
 * row value constructor builder used by e.g. PostgreSQL).
 */
@ExtendWith(MockitoExtension.class)
public class ChunkBoundaryCalculatorTest {

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

    @Test
    public void firstChunkSinglePkHasNoCondition() {
        final Table table = singlePkTable();
        final List<Column> keyColumns = table.primaryKeyColumns();
        final String expected = "SELECT \"pk1\" FROM \"s1\".\"table1\" ORDER BY \"pk1\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY";

        // The first chunk (lowerBound == null) never carries a condition, regardless of the underlying builder.
        assertThat(new ChunkBoundaryCalculator(defaultConnection(), config())
                .buildNextBoundaryQuery(table.id(), keyColumns, 1000L, null)).isEqualTo(expected);
        assertThat(new ChunkBoundaryCalculator(rowValueConstructorConnection(), config())
                .buildNextBoundaryQuery(table.id(), keyColumns, 1000L, null)).isEqualTo(expected);
    }

    @Test
    public void firstChunkThreePkHasNoCondition() {
        final Table table = threePkTable();
        final List<Column> keyColumns = table.primaryKeyColumns();
        final String expected = "SELECT \"pk1\", \"pk2\", \"pk3\" FROM \"s1\".\"table1\" " +
                "ORDER BY \"pk1\", \"pk2\", \"pk3\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY";

        assertThat(new ChunkBoundaryCalculator(defaultConnection(), config())
                .buildNextBoundaryQuery(table.id(), keyColumns, 1000L, null)).isEqualTo(expected);
        assertThat(new ChunkBoundaryCalculator(rowValueConstructorConnection(), config())
                .buildNextBoundaryQuery(table.id(), keyColumns, 1000L, null)).isEqualTo(expected);
    }

    @Test
    public void subsequentChunkSinglePkDefaultBuilder() {
        final Table table = singlePkTable();
        assertThat(new ChunkBoundaryCalculator(defaultConnection(), config())
                .buildNextBoundaryQuery(table.id(), table.primaryKeyColumns(), 1000L, new Object[]{ 1 }))
                .isEqualTo("SELECT \"pk1\" FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ?)) " +
                        "ORDER BY \"pk1\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    @Test
    public void subsequentChunkSinglePkRowValueConstructorBuilder() {
        final Table table = singlePkTable();
        assertThat(new ChunkBoundaryCalculator(rowValueConstructorConnection(), config())
                .buildNextBoundaryQuery(table.id(), table.primaryKeyColumns(), 1000L, new Object[]{ 1 }))
                .isEqualTo("SELECT \"pk1\" FROM \"s1\".\"table1\" WHERE \"pk1\" >= ? " +
                        "ORDER BY \"pk1\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    @Test
    public void subsequentChunkThreePkDefaultBuilder() {
        final Table table = threePkTable();
        assertThat(new ChunkBoundaryCalculator(defaultConnection(), config())
                .buildNextBoundaryQuery(table.id(), table.primaryKeyColumns(), 1000L, new Object[]{ 1, 5, 3 }))
                .isEqualTo("SELECT \"pk1\", \"pk2\", \"pk3\" FROM \"s1\".\"table1\" WHERE " +
                        "((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" = ?)) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    @Test
    public void subsequentChunkThreePkRowValueConstructorBuilder() {
        final Table table = threePkTable();
        assertThat(new ChunkBoundaryCalculator(rowValueConstructorConnection(), config())
                .buildNextBoundaryQuery(table.id(), table.primaryKeyColumns(), 1000L, new Object[]{ 1, 5, 3 }))
                .isEqualTo("SELECT \"pk1\", \"pk2\", \"pk3\" FROM \"s1\".\"table1\" WHERE " +
                        "ROW(\"pk1\", \"pk2\", \"pk3\") >= ROW(?, ?, ?) " +
                        "ORDER BY \"pk1\", \"pk2\", \"pk3\" OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY");
    }
}
