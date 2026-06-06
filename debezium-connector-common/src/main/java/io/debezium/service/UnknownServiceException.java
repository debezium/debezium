/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.DebeziumException;

/**
 * Indicates that a specific service was requested but was not found in the registry.
 *
 * @author Chris Cranford
 */
public class UnknownServiceException extends DebeziumException {

    private final Class<?> serviceClass;

    public UnknownServiceException(Class<?> serviceClass) {
        super("The requested service " + serviceClass.getName() + " is not found.");
        this.serviceClass = serviceClass;
    }

    public Class<?> getServiceClass() {
        return serviceClass;
    }

}
