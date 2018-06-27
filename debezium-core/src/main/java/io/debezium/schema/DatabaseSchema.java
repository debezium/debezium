/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

/**
 * The schema of a database. Provides information about the structures of the tables (collections etc.) it contains.
 *
 * @author Gunnar Morling
 *
 * @param <I>
 *            The type of {@link DataCollectionId} used by a given implementation
 */
public interface DatabaseSchema<I extends DataCollectionId> {

    void close();

    DataCollectionSchema schemaFor(I id);
}
