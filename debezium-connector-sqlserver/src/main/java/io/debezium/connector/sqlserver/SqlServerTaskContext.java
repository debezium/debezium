/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a SQL Server task
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerTaskContext extends CdcSourceTaskContext<SqlServerConnectorConfig> {

    public SqlServerTaskContext(Configuration rawConfig, SqlServerConnectorConfig config) {
        super(rawConfig, config, config.getTaskId(), config.getCustomMetricTags());
    }
}
