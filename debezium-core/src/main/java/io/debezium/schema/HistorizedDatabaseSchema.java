/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.pipeline.spi.OffsetContext;

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

    void applySchemaChange(SchemaChangeEvent schemaChange);

    void recover(OffsetContext offset);
}
