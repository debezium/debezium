/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.List;
import java.util.Map;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.rest.model.DataCollection;
import io.debezium.rest.model.FilterValidationResults;

public interface FilterValidationResource<T extends BaseSourceConnector> {

    T getConnector();

    String VALIDATE_FILTERS_ENDPOINT = "/validate/filters";

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

    default List<DataCollection> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration);
    }
}
