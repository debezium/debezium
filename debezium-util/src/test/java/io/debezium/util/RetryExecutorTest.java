/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class RetryExecutorTest {

    private static RetryExecutor executor(int maxAttempts) {
        return new RetryExecutor(maxAttempts, 1L, 2L, 2.0, Clock.SYSTEM);
    }

    @Test
    public void shouldReturnTheResultWithoutRetryingWhenTheOperationSucceeds() throws Exception {
        final AtomicInteger calls = new AtomicInteger();

        final String result = executor(3).executeWithRetry(() -> {
            calls.incrementAndGet();
            return "ok";
        }, "operation");

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(1);
    }

    @Test
    public void shouldRetryTransientFailuresUntilTheOperationSucceeds() throws Exception {
        final AtomicInteger calls = new AtomicInteger();

        final String result = executor(5).executeWithRetry(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new SQLException("transient");
            }
            return "ok";
        }, "operation");

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(3);
    }

    @Test
    public void shouldRethrowTheLastFailureAfterTheMaximumAttempts() {
        final AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> executor(3).executeWithRetry(() -> {
            throw new SQLException("failure " + calls.incrementAndGet());
        }, "operation"))
                .isInstanceOf(SQLException.class)
                .hasMessage("failure 3");
        assertThat(calls).hasValue(3);
    }

    @Test
    public void shouldKeepRetryingWhenTheAttemptsAreUnlimited() throws Exception {
        final AtomicInteger calls = new AtomicInteger();

        final String result = executor(-1).executeWithRetry(() -> {
            if (calls.incrementAndGet() < 10) {
                throw new SQLException("transient");
            }
            return "ok";
        }, "operation");

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(10);
    }

    @Test
    public void shouldNotRetryWhenASingleAttemptIsConfigured() {
        final AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> executor(1).executeWithRetry(() -> {
            calls.incrementAndGet();
            throw new SQLException("failure");
        }, "operation"))
                .isInstanceOf(SQLException.class);
        assertThat(calls).hasValue(1);
    }
}
