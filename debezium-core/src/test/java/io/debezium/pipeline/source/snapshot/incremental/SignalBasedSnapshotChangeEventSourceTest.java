/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.Optional;

import org.fest.assertions.Assertions;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class SignalBasedSnapshotChangeEventSourceTest {

    protected RelationalDatabaseConnectorConfig config() {
        return new RelationalDatabaseConnectorConfig(
                Configuration.create().with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                        .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core").build(),
                null, null, 0, ColumnFilterMode.CATALOG, true) {
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
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        source.setContext(context);
        final Column pk1 = Column.editor().name("pk1").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        Assertions.assertThat(source.buildChunkQuery(table, Optional.empty())).isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        Assertions.assertThat(source.buildChunkQuery(table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) AND NOT (\"pk1\" > ?) ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryOnePkColumnWithAdditionalCondition() {
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        source.setContext(context);
        final Column pk1 = Column.editor().name("pk1").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(pk1)
                .addColumn(val1)
                .addColumn(val2)
                .setPrimaryKeyNames("pk1").create();
        Assertions.assertThat(source.buildChunkQuery(table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        Assertions.assertThat(source.buildChunkQuery(table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) AND NOT (\"pk1\" > ?) AND \"val1\"=foo ORDER BY \"pk1\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumns() {
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        source.setContext(context);
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
        Assertions.assertThat(source.buildChunkQuery(table, Optional.empty())).isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        Assertions.assertThat(source.buildChunkQuery(table, Optional.empty())).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testBuildQueryThreePkColumnsWithAdditionalCondition() {
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final IncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        source.setContext(context);
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
        Assertions.assertThat(source.buildChunkQuery(table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
        context.nextChunkPosition(new Object[]{ 1, 5 });
        context.maximumKey(new Object[]{ 10, 50 });
        Assertions.assertThat(source.buildChunkQuery(table, Optional.of("\"val1\"=foo"))).isEqualTo(
                "SELECT * FROM \"s1\".\"table1\" WHERE ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND NOT ((\"pk1\" > ?) OR (\"pk1\" = ? AND \"pk2\" > ?) OR (\"pk1\" = ? AND \"pk2\" = ? AND \"pk3\" > ?)) AND \"val1\"=foo ORDER BY \"pk1\", \"pk2\", \"pk3\" LIMIT 1024");
    }

    @Test
    public void testMaxQuery() {
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1")).addColumn(pk1).addColumn(pk2)
                .addColumn(val1).addColumn(val2).setPrimaryKeyNames("pk1", "pk2").create();
        Assertions.assertThat(source.buildMaxPrimaryKeyQuery(table, Optional.empty()))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\" DESC, \"pk2\" DESC LIMIT 1");
    }

    @Test
    public void testMaxQueryWithAdditionalCondition() {
        final SignalBasedIncrementalSnapshotChangeEventSource<? extends Partition, TableId> source = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config(), new JdbcConnection(config().getJdbcConfig(), config -> null, "\"", "\""), null, null, null, SnapshotProgressListener.NO_OP(),
                DataChangeEventListener.NO_OP());
        final Column pk1 = Column.editor().name("pk1").create();
        final Column pk2 = Column.editor().name("pk2").create();
        final Column val1 = Column.editor().name("val1").create();
        final Column val2 = Column.editor().name("val2").create();
        final Table table = Table.editor().tableId(new TableId(null, "s1", "table1")).addColumn(pk1).addColumn(pk2)
                .addColumn(val1).addColumn(val2).setPrimaryKeyNames("pk1", "pk2").create();
        Assertions.assertThat(source.buildMaxPrimaryKeyQuery(table, Optional.of("\"val1\"=foo")))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE \"val1\"=foo ORDER BY \"pk1\" DESC, \"pk2\" DESC LIMIT 1");
    }
}
