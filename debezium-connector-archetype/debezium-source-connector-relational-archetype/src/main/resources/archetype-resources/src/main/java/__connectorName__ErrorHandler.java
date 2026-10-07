/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for the ${connectorName} connector.
 */
public class ${connectorName}ErrorHandler extends ErrorHandler {

    public ${connectorName}ErrorHandler(CommonConnectorConfig connectorConfig,
                                        ChangeEventQueue<?> queue,
                                        ErrorHandler replacedErrorHandler) {
        super(${connectorName}SourceConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        // A lost or refused JDBC connection surfaces as a SQLException, which should restart the
        // connector instead of failing it. Add your driver's own exception types if they differ.
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }
}
