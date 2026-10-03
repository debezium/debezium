/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for the ${connectorName} connector.
 */
public class ${connectorName}ErrorHandler extends ErrorHandler {

    public ${connectorName}ErrorHandler(CommonConnectorConfig connectorConfig,
                                        ChangeEventQueue<?> queue,
                                        ErrorHandler replacedErrorHandler) {
        super(${connectorName}SourceConnector.class, connectorConfig, queue, replacedErrorHandler);
    }
}
