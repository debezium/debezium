/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a SQL Server task
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerTaskContext extends CdcSourceTaskContext {

    public SqlServerTaskContext(SqlServerConnectorConfig config) {
        super(config, config.getTaskId(), config.getCustomMetricTags());
    }
}
