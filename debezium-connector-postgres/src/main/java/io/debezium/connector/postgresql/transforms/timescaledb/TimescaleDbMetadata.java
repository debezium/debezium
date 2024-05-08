/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.Closeable;
import java.util.Optional;

import io.debezium.relational.TableId;

/**
 * A repository resolving TimescaleDB related metadata.
 * The implementations can differ but the basic one is expected to just read TimescaleDB information
 * schema and resolve metadata according to it.
 *
 * @author Jiri Pechanec
 *
 */
public interface TimescaleDbMetadata extends Closeable {

    /**
     * @param schemaName
     * @return true if the schema name is one used by TimescaleDB to store data
     */
    boolean isTimescaleDbSchema(String schemaName);

    /**
     * Returns the {@link TableId} to which given chunk belong. Empty if this is not TimescaleDB chunk identifier.
     * Resolution if the identifier is chunk or not is based on a preconfigured list of schemas.
     */
    Optional<TableId> hypertableId(TableId chunkId);

    /**
     * Returns the {@link TableId} that represents continuous aggregate view for which the hypertable is backend.
     * Empty if hypertable is not associated with a continuous aggregate view.
     */
    Optional<TableId> aggregateId(TableId hypertableId);
}
