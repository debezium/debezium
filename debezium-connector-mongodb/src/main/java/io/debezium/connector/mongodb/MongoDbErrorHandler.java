/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for MongoDB.
 *
 * @author John Graf
 */
public class MongoDbErrorHandler extends ErrorHandler {

    public MongoDbErrorHandler(MongoDbConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(MongoDbConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof org.apache.kafka.connect.errors.ConnectException) {
            Throwable cause = throwable.getCause();
            while ((cause != null) && (cause != throwable)) {
                if (cause instanceof com.mongodb.MongoSocketException ||
                        cause instanceof com.mongodb.MongoTimeoutException ||
                        cause instanceof com.mongodb.MongoExecutionTimeoutException) {
                    return true;
                }
                else {
                    cause = cause.getCause();
                }
            }
        }

        return false;
    }
}
