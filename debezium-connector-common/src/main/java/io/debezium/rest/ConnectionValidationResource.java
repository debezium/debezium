/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.Map;

import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import io.debezium.rest.model.ValidationResults;

public interface ConnectionValidationResource extends ConnectorAware {

    String VALIDATE_CONNECTION_ENDPOINT = "/validate/connection";

    @PUT
    @Path(VALIDATE_CONNECTION_ENDPOINT)
    default ValidationResults validateConnectionProperties(Map<String, ?> properties) {
        // switch classloader to the connector specific classloader in order to load dependencies required to validate the connector config
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getConnector().getClass().getClassLoader());
        ValidationResults validationResults = ConnectorConfigValidator.validateConfig(getConnector(), properties);
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return validationResults;
    }
}
