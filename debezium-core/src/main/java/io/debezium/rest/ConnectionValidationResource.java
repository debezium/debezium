/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.Map;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.ValidationResults;

public interface ConnectionValidationResource {

    Connector getConnector();

    String VALIDATE_CONNECTION_ENDPOINT = "/validate/connection";

    @PUT
    @Path(VALIDATE_CONNECTION_ENDPOINT)
    default ValidationResults validateConnectionProperties(Map<String, ?> properties) {
        // switch classloader to the connector specific classloader in order to load dependencies required to validate the connector config
        ValidationResults validationResults;
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getConnector().getClass().getClassLoader());
        validationResults = new ValidationResults(getConnector(), properties);
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return validationResults;
    }
}
