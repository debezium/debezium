/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Collection;
import java.util.function.Predicate;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A database schema that is historized, i.e. it undergoes schema changes and can be recovered from a persistent schema
 * history.
 *
 * @author Gunnar Morling
 *
 * @param <I>
 *            The collection id type of this schema
 */
public interface HistorizedDatabaseSchema<I extends DataCollectionId> extends DatabaseSchema<I> {

    @FunctionalInterface
    interface SchemaChangeEventConsumer {

        void consume(SchemaChangeEvent event, Collection<TableId> tableIds);

        SchemaChangeEventConsumer NOOP = (x, y) -> {
        };
    }

    void applySchemaChange(SchemaChangeEvent schemaChange);

    default void recover(Partition partition, OffsetContext offset) throws InterruptedException {
        recover(Offsets.of(partition, offset));
    }

    void recover(Offsets<?, ?> offsets) throws InterruptedException;

    void initializeStorage();

    Predicate<String> ddlFilter();

    boolean skipUnparseableDdlStatements();

    boolean storeOnlyCapturedTables();

    boolean storeOnlyCapturedDatabases();

    SchemaHistory getSchemaHistory();
}
