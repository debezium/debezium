/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

/**
 * Provides factory methods for obtaining {@link DataCollectionFilter} instances as per the current connector configuration.
 *
 * @author Gunnar Morling
 */
public interface DataCollectionFilters {

    public DataCollectionFilter<?> dataCollectionFilter();

    @FunctionalInterface
    public interface DataCollectionFilter<T extends DataCollectionId> {
        boolean isIncluded(T id);
    }
}
