/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class ThreadsTest {

    @Test
    public void shouldCompleteSuccessfullyWithinTimeout() throws Exception {
        AtomicBoolean taskCompleted = new AtomicBoolean(false);
        Runnable operation = () -> {
            try {
                Thread.sleep(100);
                taskCompleted.set(true);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Threads.runWithTimeout(
                ThreadsTest.class,
                operation,
                Duration.ofMillis(1000),
                "test-connector",
                "test-operation");

        assertTrue(taskCompleted.get());
    }

    @Test
    public void shouldTimeoutWhenOperationTakesTooLong() {
        Runnable operation = () -> {
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        assertThrows(TimeoutException.class, () -> Threads.runWithTimeout(
                ThreadsTest.class,
                operation,
                Duration.ofMillis(500),
                "test-connector",
                "test-operation"));
    }

    @Test
    public void shouldPropagateOperationException() {
        Runnable operation = () -> {
            throw new RuntimeException("Test exception");
        };

        Exception exception = assertThrows(Exception.class, () -> Threads.runWithTimeout(
                ThreadsTest.class,
                operation,
                Duration.ofMillis(1000),
                "test-connector",
                "test-operation"));

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertTrue(exception.getCause().getMessage().contains("Test exception"));
    }

    @Test
    public void shouldPropagateWrappedOperationException() {
        Runnable operation = () -> {
            SQLException se = new SQLException("Test exception");
            throw new RuntimeException(se);
        };

        Exception exception = assertThrows(Exception.class, () -> Threads.runWithTimeout(
                ThreadsTest.class,
                operation,
                Duration.ofMillis(1000),
                "test-connector",
                "test-operation"));

        assertTrue(exception instanceof Exception);
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertTrue(exception.getCause().getCause() instanceof SQLException);
        assertTrue(exception.getCause().getCause().getMessage().contains("Test exception"));
    }

    @Test
    public void shouldHandleInterruptedException() {
        Runnable operation = () -> {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Should be interrupted");
        };

        assertThrows(Exception.class, () -> Threads.runWithTimeout(
                ThreadsTest.class,
                operation,
                Duration.ofMillis(1000),
                "test-connector",
                "test-operation"));
    }
}