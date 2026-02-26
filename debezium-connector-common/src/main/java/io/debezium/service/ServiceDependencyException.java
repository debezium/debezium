/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.DebeziumException;

/**
 * Exception that identifies a service dependency injection failure.
 *
 * @author Chris Cranford
 */
public class ServiceDependencyException extends DebeziumException {

    public ServiceDependencyException(String message) {
        super(message);
    }

}
