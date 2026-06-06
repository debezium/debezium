/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Functional interface for supplying collections of captured table identifiers.
 * This interface provides access to the set of tables that have been captured
 * for data collection purposes.
 *
 * @author Mario Fiore Vitale
 */
@FunctionalInterface
public interface CapturedTablesSupplier {

    /**
     * Gets a collection of identifiers for all captured tables.
     *
     * @return a collection containing DataCollectionId instances representing
     *         the captured tables, or instances of any subtype of DataCollectionId
     */
    Collection<? extends DataCollectionId> getCapturedTables();
}
