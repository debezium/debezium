/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.model;

import java.util.List;

import org.apache.kafka.common.config.Config;

public class FilterValidationResults extends ValidationResults {

    public final List<DataCollection> matchingCollections;

    public FilterValidationResults(Config validatedConfig, MatchingCollectionsParameter matchingCollections) {
        super(validatedConfig);
        if (Status.INVALID == this.status) {
            this.matchingCollections = List.of();
        }
        else {
            this.matchingCollections = matchingCollections.get();
        }
    }

    /**
     * Functional interface to provide the list of matching tables or collections with an anonymous function / lambda with lazy evaluation.
     */
    public interface MatchingCollectionsParameter {
        /**
         * @return the list of matching tables or collections that match the connector config based filters / filter conditions.
         */
        List<DataCollection> get();
    }

}
