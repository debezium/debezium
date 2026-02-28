/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.List;
import java.util.Map;

import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import io.debezium.config.Configuration;
import io.debezium.metadata.CollectionId;
import io.debezium.rest.model.FilterValidationResults;

public interface FilterValidationResource extends ConnectorAware {

    String VALIDATE_FILTERS_ENDPOINT = "/validate/filters";

    List<CollectionId> getMatchingCollections(Configuration configuration);

    @PUT
    @Path(VALIDATE_FILTERS_ENDPOINT)
    default FilterValidationResults validateFiltersProperties(Map<String, ?> properties) {
        // switch classloader to the connector specific classloader in order to load dependencies required to validate the connector config
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getConnector().getClass().getClassLoader());
        FilterValidationResults validationResults = ConnectorConfigValidator.validateFilterConfig(
                getConnector(), properties, () -> getMatchingCollections(Configuration.from(properties)));
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return validationResults;
    }

}
