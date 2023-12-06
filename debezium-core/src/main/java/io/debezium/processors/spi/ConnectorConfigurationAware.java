/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

import io.debezium.config.CommonConnectorConfig;

/**
 * Contract that allows injecting a {@link CommonConnectorConfig}.
 *
 * @author Chris Cranford
 */
public interface ConnectorConfigurationAware {
    void setConnectorConfig(CommonConnectorConfig connectorConfig);
}
