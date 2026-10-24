/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

public class MariaDbChunkQueryBuilderTest {

    private static final TableId TABLE_ID = new TableId("test", null, "year2");

    @Test
    public void shouldReadNonKeyYear2ColumnAsYear() {
        final Table table = tableWithPrimaryKey("id", column("id"), year2Column("y"));

        assertThat(chunkQueryBuilder().buildChunkQuery(context(), table, Optional.empty()))
                .isEqualTo("SELECT `id`, YEAR(`y`) AS `y` FROM `test`.`year2` ORDER BY `id` LIMIT 1024");
    }

    @Test
    public void shouldReadKeyYear2ColumnAsStored() {
        final Table table = tableWithPrimaryKey("y", column("id"), year2Column("y"));

        assertThat(chunkQueryBuilder().buildChunkQuery(context(), table, Optional.empty()))
                .isEqualTo("SELECT * FROM `test`.`year2` ORDER BY `y` LIMIT 1024");
    }

    @Test
    public void shouldReadSurrogateKeyYear2ColumnAsStored() {
        final Table table = tableWithPrimaryKey("id", column("id"), year2Column("y"));
        final SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("correlation", List.of("test.year2"), List.of(), "y");

        assertThat(chunkQueryBuilder().buildChunkQuery(context, table, Optional.empty()))
                .isEqualTo("SELECT * FROM `test`.`year2` ORDER BY `y` LIMIT 1024");
    }

    @Test
    public void shouldReadYear4ColumnAsStored() {
        final Table table = tableWithPrimaryKey("id", column("id"), yearColumn("y", 4));

        assertThat(chunkQueryBuilder().buildChunkQuery(context(), table, Optional.empty()))
                .isEqualTo("SELECT * FROM `test`.`year2` ORDER BY `id` LIMIT 1024");
    }

    @Test
    public void shouldReadFilteredYear2ColumnAsYear() {
        final Table table = tableWithPrimaryKey("id", column("id"), year2Column("y"), column("name"));
        final ChunkQueryBuilder<TableId> chunkQueryBuilder = chunkQueryBuilder(builder -> builder
                .with(RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST, "test.year2.y"));

        assertThat(chunkQueryBuilder.buildChunkQuery(context(), table, Optional.empty()))
                .isEqualTo("SELECT `id`, YEAR(`y`) AS `y` FROM `test`.`year2` ORDER BY `id` LIMIT 1024");
    }

    private ChunkQueryBuilder<TableId> chunkQueryBuilder() {
        return chunkQueryBuilder(builder -> builder);
    }

    private ChunkQueryBuilder<TableId> chunkQueryBuilder(UnaryOperator<Configuration.Builder> customizer) {
        final Configuration config = customizer.apply(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "core")
                .with(RelationalDatabaseConnectorConfig.DATABASE_NAME, "test"))
                .build();
        final MariaDbConnectorConfig connectorConfig = new MariaDbConnectorConfig(config);
        return new MariaDbChunkQueryBuilder<>(connectorConfig, new MariaDbStyleJdbcConnection(connectorConfig.getJdbcConfig()));
    }

    private SignalBasedIncrementalSnapshotContext<TableId> context() {
        return new SignalBasedIncrementalSnapshotContext<>();
    }

    private Table tableWithPrimaryKey(String primaryKey, Column... columns) {
        final TableEditor editor = Table.editor().tableId(TABLE_ID);
        for (Column column : columns) {
            editor.addColumn(column);
        }
        return editor.setPrimaryKeyNames(primaryKey).create();
    }

    private Column column(String name) {
        return Column.editor().name(name).optional(false).create();
    }

    private Column year2Column(String name) {
        return yearColumn(name, 2);
    }

    private Column yearColumn(String name, int length) {
        return Column.editor().name(name).type("YEAR").length(length).optional(false).create();
    }

    private static class MariaDbStyleJdbcConnection extends JdbcConnection {

        MariaDbStyleJdbcConnection(JdbcConfiguration config) {
            super(config, connectionFactory -> null, "`", "`");
        }

        @Override
        public String quotedTableIdString(TableId tableId) {
            return tableId.toQuotedString('`');
        }
    }
}
