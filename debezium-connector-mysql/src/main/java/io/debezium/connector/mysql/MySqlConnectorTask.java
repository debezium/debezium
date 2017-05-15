/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.extensions.restart.SelfRestartingTask;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
public final class MySqlConnectorTask extends SelfRestartingTask<DefaultMySqlConnectorTask> {

    public MySqlConnectorTask() {
        super(DefaultMySqlConnectorTask.class);
    }
}
