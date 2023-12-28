/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.DisconnectEvent;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.pipeline.EventDispatcher;

/**
 * Factory class providing access to different configurations of {@link MongoDbConnection} instances.
 * This should be the singular way of creating instances of {@link MongoDbConnection} across the codebase.
 */
public final class MongoDbConnections {

    public static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnections.class);

    /**
     * Creates {@link MongoDbConnection.ErrorHandler} that just wraps any exception into {@link DebeziumException}
     *
     * @return error handler
     */
    public static MongoDbConnection.ErrorHandler throwingErrorHandler() {
        return (desc, error) -> {
            throw new DebeziumException(desc, error);
        };
    }

    /**
     * Creates {@link MongoDbConnection.ErrorHandler} that
     * <ul>
     *     <li>Logs the error</li>
     *     <li>Notifies dispatcher in case of of authorisation error</li>
     *     <li>Rethrows the error as {@link DebeziumException}</li>
     * </ul>
     *
     * @return error handler
     */
    public static MongoDbConnection.ErrorHandler eventSourcingErrorHandler(EventDispatcher<MongoDbPartition, CollectionId> dispatcher, MongoDbPartition partition) {
        return (desc, error) -> {
            if (error.getMessage() == null || !error.getMessage().startsWith(MongoDbConnection.AUTHORIZATION_FAILURE_MESSAGE)) {
                dispatcher.dispatchConnectorEvent(partition, new DisconnectEvent());
            }
            LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
            throw new DebeziumException("Error while attempting to " + desc, error);
        };
    }

    /**
     * Creates {@link MongoDbConnection} with error handler provided by {@link MongoDbConnections#throwingErrorHandler()}
     *
     * @param configuration connector config
     * @return instance of {@link MongoDbConnection}
     */
    public static MongoDbConnection create(Configuration configuration) {
        return new MongoDbConnection(configuration, throwingErrorHandler());
    }

    /**
     * Creates {@link MongoDbConnection} with arbitrary error handler
     *
     * @param configuration connector config
     * @param errorHandler error handler
     * @return instance of {@link MongoDbConnection}
     */
    public static MongoDbConnection create(Configuration configuration, MongoDbConnection.ErrorHandler errorHandler) {
        return new MongoDbConnection(configuration, errorHandler);
    }

    /**
     * Creates {@link MongoDbConnection} with error handler provided by {@link MongoDbConnections#eventSourcingErrorHandler(EventDispatcher, MongoDbPartition)}
     *
     * @param configuration connector config
     * @param dispatcher event dispatcher
     * @param partition mongodb partition
     * @return instance of {@link MongoDbConnection}
     */
    public static MongoDbConnection create(Configuration configuration,
                                           EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                           MongoDbPartition partition) {
        return new MongoDbConnection(configuration, eventSourcingErrorHandler(dispatcher, partition));
    }

    private MongoDbConnections() {
        // intentionally private
    }
}
