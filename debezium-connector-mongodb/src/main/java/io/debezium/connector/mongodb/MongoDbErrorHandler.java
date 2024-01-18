/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.io.IOException;
import java.util.Set;

import com.mongodb.MongoException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for MongoDB.
 *
 * @author John Graf
 */
public class MongoDbErrorHandler extends ErrorHandler {

    public MongoDbErrorHandler(MongoDbConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(MongoDbConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, MongoException.class);
    }
}
