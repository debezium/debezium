/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata.ExtendedTable;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableImpl;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.util.Collect;

/**
 * Verifies that a schema history configured with a connector's metadata factory records and recovers
 * connector-specific metadata, and that the standard configuration keeps the standard serializer.
 *
 * @author Chris Cranford
 */
public class SchemaHistoryMetadataFactoryTest {

    private final TableId tableId = new TableId("db", "schema", "orders");
    private final Map<String, Object> source = Collect.linkMapOf("server", "abc");
    private final Map<String, Object> position = Collect.linkMapOf("file", "a.log", "pos", 1);
    private static final String IGNORED_FACTORY_WARNING = "ignores the connector's metadata factory";
    private MemorySchemaHistory history;

    @BeforeEach
    public void beforeEach() {
        history = new MemorySchemaHistory();
    }

    @AfterEach
    public void afterEach() {
        history.stop();
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldRecoverConnectorMetadataThroughFactorySerializer() throws InterruptedException {
        history.configure(Configuration.empty(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false,
                ExtendedRelationalMetadataFactory.INSTANCE);
        history.start();

        final ExtendedTable table = ExtendedMetadata.table(tableId, "orders", "us");
        history.record(source, position, "db", "schema", "CREATE TABLE orders (id NUMBER)", new TableChanges().create(table), Instant.now());

        final Tables tables = new Tables(false, ExtendedRelationalMetadataFactory.INSTANCE);
        history.recover(source, position, tables, null);

        final Table recovered = tables.forTable(tableId);
        assertThat(recovered).isEqualTo(table);
        assertThat(recovered.unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(recovered.columnWithName("id").unwrap(ExtendedColumn.class).region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepStandardSerializerWithoutFactory() throws InterruptedException {
        history.configure(Configuration.empty(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
        history.start();

        final Table table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("id").type("NUMBER").jdbcType(Types.NUMERIC).optional(false).create())
                .setPrimaryKeyNames("id")
                .create();
        history.record(source, position, "db", "schema", "CREATE TABLE orders (id NUMBER)", new TableChanges().create(table), Instant.now());

        final Tables tables = new Tables();
        history.recover(source, position, tables, null);

        assertThat(tables.forTable(tableId)).isExactlyInstanceOf(TableImpl.class);
        assertThat(tables.forTable(tableId)).isEqualTo(table);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldWarnWhenHistoryIgnoresConnectorFactory() {
        final LogInterceptor logInterceptor = new LogInterceptor(SchemaHistory.class);
        final SchemaHistory forwarding = new ForwardingSchemaHistory(history);

        forwarding.configure(Configuration.empty(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false,
                ExtendedRelationalMetadataFactory.INSTANCE);
        forwarding.start();

        assertThat(logInterceptor.containsWarnMessage(IGNORED_FACTORY_WARNING)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldNotWarnWhenHistoryIgnoresDefaultFactory() {
        final LogInterceptor logInterceptor = new LogInterceptor(SchemaHistory.class);
        final SchemaHistory forwarding = new ForwardingSchemaHistory(history);

        forwarding.configure(Configuration.empty(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false,
                DefaultRelationalMetadataFactory.INSTANCE);
        forwarding.start();

        assertThat(logInterceptor.containsMessage(IGNORED_FACTORY_WARNING)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldNotWarnWhenHistoryAcceptsConnectorFactory() {
        final LogInterceptor logInterceptor = new LogInterceptor(SchemaHistory.class);

        history.configure(Configuration.empty(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false,
                ExtendedRelationalMetadataFactory.INSTANCE);
        history.start();

        assertThat(logInterceptor.containsMessage(IGNORED_FACTORY_WARNING)).isFalse();
    }

    /**
     * A history that implements {@link SchemaHistory} directly and only the four-argument {@code configure},
     * like an implementation written before the metadata factory existed.
     */
    private static class ForwardingSchemaHistory implements SchemaHistory {
        private final SchemaHistory delegate;

        ForwardingSchemaHistory(SchemaHistory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
            delegate.configure(config, comparator, listener, useCatalogBeforeSchema);
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl) {
            delegate.record(source, position, databaseName, ddl);
        }

        @Override
        public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName, String ddl, TableChanges changes,
                           Instant timestamp) {
            delegate.record(source, position, databaseName, schemaName, ddl, changes, timestamp);
        }

        @Override
        public void recover(Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) throws InterruptedException {
            delegate.recover(offsets, schema, ddlParser);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public boolean exists() {
            return delegate.exists();
        }

        @Override
        public boolean storageExists() {
            return delegate.storageExists();
        }

        @Override
        public void initializeStorage() {
            delegate.initializeStorage();
        }
    }
}
