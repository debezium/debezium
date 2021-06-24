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

    String NO_CAPTURED_DATA_COLLECTIONS_WARNING = "After applying the include/exclude list filters, no changes will be captured. Please check your configuration!";

    void close();

    DataCollectionSchema schemaFor(I id);

    /**
     * Indicates whether or not table names are guaranteed to be fully present, regardless of whether or not a
     * snapshot has been performed.
     *
     * @return boolean indicating if table names are present
     */
    boolean tableInformationComplete();

    default void assureNonEmptySchema() {
    }
}
