/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.rest.model.FilterValidationResults;
import io.debezium.rest.model.ValidationResults;

public class ConnectorConfigValidator {

    public static ValidationResults validateConfig(Connector connector, Map<String, ?> properties) {
        return new ValidationResults(connector.validate(convertPropertiesToStrings(properties)));
    }

    public static FilterValidationResults validateFilterConfig(Connector connector, Map<String, ?> properties,
                                                               FilterValidationResults.MatchingCollectionsParameter matchingCollections) {
        return new FilterValidationResults(connector.validate(convertPropertiesToStrings(properties)), matchingCollections);
    }

    static Map<String, String> convertPropertiesToStrings(Map<String, ?> properties) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
    }

}
