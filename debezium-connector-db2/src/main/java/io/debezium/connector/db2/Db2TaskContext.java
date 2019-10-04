/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a DB2 task
 *
 * @author Jiri Pechanec
 *
 */
public class Db2TaskContext extends CdcSourceTaskContext {

    public Db2TaskContext(Db2ConnectorConfig config, Db2DatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
