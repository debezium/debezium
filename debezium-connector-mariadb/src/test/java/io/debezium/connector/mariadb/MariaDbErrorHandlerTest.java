/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.DataChangeEvent;

public class MariaDbErrorHandlerTest {

    private static final int ER_MASTER_FATAL_ERROR_READING_BINLOG = 1236;

    private final MariaDbErrorHandler errorHandler = new MariaDbErrorHandler(
            new MariaDbConnectorConfig(Configuration.create()
                    .with(MariaDbConnectorConfig.HOSTNAME, "localhost")
                    .with(MariaDbConnectorConfig.PORT, 3306)
                    .with(MariaDbConnectorConfig.USER, "mariadbuser")
                    .with(MariaDbConnectorConfig.PASSWORD, "mariadbpw")
                    .with(MariaDbConnectorConfig.SERVER_ID, 18765)
                    .with(MariaDbConnectorConfig.TOPIC_PREFIX, "mariadb-server")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>().build(), null);

    @Test
    @FixFor("debezium/dbz#2611")
    void ioExceptionIsRetriable() {
        assertThat(errorHandler.isRetriable(new IOException("connection reset"))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void wrappedEofExceptionIsRetriable() {
        final Throwable throwable = new DebeziumException(new EOFException("Failed to read next byte from position 1"));
        assertThat(errorHandler.isRetriable(throwable)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void sqlExceptionWithoutNonRetriableCodeIsRetriable() {
        assertThat(errorHandler.isRetriable(new SQLException("communications link failure", "08S01", 0))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void binlogReadErrorIsNotRetriable() {
        final SQLException sqlException = new SQLException("could not find next log", "HY000", ER_MASTER_FATAL_ERROR_READING_BINLOG);
        assertThat(errorHandler.isRetriable(sqlException)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void wrappedBinlogReadErrorIsNotRetriable() {
        final SQLException sqlException = new SQLException("could not find next log", "HY000", ER_MASTER_FATAL_ERROR_READING_BINLOG);
        assertThat(errorHandler.isRetriable(new DebeziumException(sqlException))).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#694")
    void binlogStreamingReadErrorIsNotRetriable() {
        // The binlog client reports an error that the server sends while streaming as a ServerException, an IOException
        final ServerException serverException = new ServerException(
                "Cannot replicate GTID-transaction when @@GLOBAL.GTID_MODE = OFF", ER_MASTER_FATAL_ERROR_READING_BINLOG, "HY000");
        assertThat(errorHandler.isRetriable(new DebeziumException("Error code: 1236; SQLSTATE: HY000.", serverException))).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#694")
    void binlogStreamingErrorWithoutNonRetriableCodeIsRetriable() {
        final ServerException serverException = new ServerException("Got timeout reading communication packets", 1159, "08S01");
        assertThat(errorHandler.isRetriable(new DebeziumException("Error code: 1159; SQLSTATE: 08S01.", serverException))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void unrelatedExceptionIsNotRetriable() {
        assertThat(errorHandler.isRetriable(new NullPointerException())).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2611")
    void nullThrowableIsNotRetriable() {
        assertThat(errorHandler.isRetriable(null)).isFalse();
    }
}
