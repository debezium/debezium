/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogSourceTask.BinlogHeartbeatErrorHandler;

/**
 * Unit tests for {@link BinlogSourceTask.BinlogHeartbeatErrorHandler}.
 */
public class BinlogSourceTaskTest {

    private final BinlogHeartbeatErrorHandler errorHandler = new BinlogHeartbeatErrorHandler();

    @Test
    void shouldNotFailWhenSqlStateIsNotReported() {
        // A driver is not required to populate the SQL state; a connection-level failure
        // commonly leaves it unset. The handler must not dereference it unconditionally.
        final SQLException exception = new SQLException("Communications link failure");

        assertThatCode(() -> errorHandler.onError(exception)).doesNotThrowAnyException();
    }

    @Test
    void shouldThrowWhenDatabaseAccessIsDenied() {
        final SQLException exception = new SQLException("Access denied", "42000");

        assertThatThrownBy(() -> errorHandler.onError(exception))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("42000")
                .hasCause(exception);
    }

    @Test
    void shouldThrowWhenDatabaseIsNotSelected() {
        final SQLException exception = new SQLException("No database selected", "3D000");

        assertThatThrownBy(() -> errorHandler.onError(exception))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("3D000")
                .hasCause(exception);
    }

    @Test
    void shouldTolerateUnrecognizedSqlState() {
        // Anything that is not a permanent configuration fault is left for the caller to log,
        // so that a transient failure does not terminate the task.
        final SQLException exception = new SQLException("Communications link failure", "08S01");

        assertThatCode(() -> errorHandler.onError(exception)).doesNotThrowAnyException();
    }
}
