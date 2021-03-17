/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.sql.SQLException;

public interface HeartbeatErrorHandler {

    HeartbeatErrorHandler DEFAULT_NOOP_ERRORHANDLER = exception -> {
    };

    void onError(SQLException exception) throws RuntimeException;

}
