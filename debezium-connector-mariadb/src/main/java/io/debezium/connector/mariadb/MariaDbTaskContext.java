/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogTaskContext;

/**
 * A state (context) associated with a MariaDB connector task.
 *
 * @author Chris Cranford
 */
public class MariaDbTaskContext extends BinlogTaskContext<MariaDbDatabaseSchema> {
    public MariaDbTaskContext(MariaDbConnectorConfig config) {
        super(config);
    }
}
