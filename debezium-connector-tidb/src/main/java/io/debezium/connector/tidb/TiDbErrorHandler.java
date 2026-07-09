/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.io.IOException;
import java.util.Set;

import org.apache.kafka.common.errors.RetriableException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for the TiDB connector.
 *
 * @author Aviral Srivastava
 */
public class TiDbErrorHandler extends ErrorHandler {

    public TiDbErrorHandler(TiDbConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(TiDbConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, RetriableException.class);
    }
}
