/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import io.debezium.jdbc.JdbcConnection;

/**
 * Defines a contract for providing a connection to the {@link DatabaseHeartbeat}.
 *
 * @author Chris Cranford
 */
public interface HeartbeatConnectionProvider {
    JdbcConnection get();
}
