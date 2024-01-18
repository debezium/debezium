/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides factory methods for obtaining {@link DataCollectionFilter} instances as per the current connector configuration.
 *
 * @author Gunnar Morling
 */
public interface DataCollectionFilters {

    DataCollectionFilter<?> dataCollectionFilter();

    @FunctionalInterface
    interface DataCollectionFilter<T extends DataCollectionId> {
        boolean isIncluded(T id);
    }
}
