/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.doc.FixFor;
import io.debezium.storage.jdbc.RetriableConnection.ConnectionConsumer;

public class RetriableConnectionTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @FixFor("debezium/dbz#2244")
    public void shouldStopRetryingFailingOperationAfterMaxRetries() throws IOException, SQLException {
        File dbFile = File.createTempFile("retriable-", ".db");
        dbFile.deleteOnExit();
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        int maxRetryCount = 3;

        try (RetriableConnection connection = new RetriableConnection(url, "user", "pass", Duration.ofMillis(1), maxRetryCount)) {
            AtomicInteger calls = new AtomicInteger();
            // The connection itself stays healthy (reconnection always succeeds); only the
            // operation keeps failing. Before the fix this reconnected and re-ran the failing
            // operation forever. It must now give up after maxRetryCount attempts and rethrow.
            ConnectionConsumer alwaysFailing = conn -> {
                calls.incrementAndGet();
                throw new SQLException("persistent operation failure");
            };

            assertThatThrownBy(() -> connection.executeWithRetry(alwaysFailing, "always-failing", false))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("persistent operation failure");

            assertThat(calls.get()).isEqualTo(maxRetryCount);
        }
    }
}
