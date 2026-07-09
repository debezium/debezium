/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * The context of the single task of the TiDB connector.
 *
 * @author Aviral Srivastava
 */
public class TiDbTaskContext extends CdcSourceTaskContext<TiDbConnectorConfig> {

    public TiDbTaskContext(Configuration rawConfig, TiDbConnectorConfig connectorConfig) {
        super(rawConfig, connectorConfig, connectorConfig.getCustomMetricTags());
    }
}
