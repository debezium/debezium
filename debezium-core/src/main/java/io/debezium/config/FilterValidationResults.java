/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.List;
import java.util.Map;

import io.debezium.rest.DataCollection;

public class FilterValidationResults extends ValidationResults {

    public final List<DataCollection> matchingCollections;

    public FilterValidationResults(ConnectorParameter connector, Map<String, ?> properties, MatchingCollectionsParameter matchingCollections) {
        super(connector, properties);
        if (Status.INVALID == this.status) {
            this.matchingCollections = List.of();
        }
        else {
            this.matchingCollections = matchingCollections.get();
        }
    }

    public interface MatchingCollectionsParameter {
        List<DataCollection> get();
    }

}
