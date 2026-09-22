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

    @Override
    protected boolean isRetriable(Throwable throwable) {
        return !isRequiredImageMissing(throwable) && super.isRetriable(throwable);
    }

    /**
     * Missing required images cannot be recovered by reopening the stream, even if image recording is enabled later.
     * Check the server message as NoMatchingDocument (47) is also used by operations unrelated to change stream images.
     */
    public static boolean isRequiredImageMissing(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof MongoException mongoException && mongoException.getCode() == 47) {
                final var message = mongoException.getMessage();
                if (message != null && (message.contains("Change stream was configured to require a pre-image")
                        || message.contains("Change stream was configured to require a post-image"))) {
                    return true;
                }
            }
        }
        return false;
    }
}
