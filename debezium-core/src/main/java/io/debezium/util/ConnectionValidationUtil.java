/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionValidationUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionValidationUtil.class);

    public static void runWithTimeout(Class<?> connectorClass, Runnable validationTask, long timeoutMs, String connectorName) throws Exception {
        ExecutorService executor = Threads.newSingleThreadExecutor(connectorClass, connectorName, "connection-validation");
        Future<?> future = executor.submit(validationTask);
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            LOGGER.error("Connection validation timed out after {} ms", timeoutMs);
            future.cancel(true);
            throw e;
        }
        catch (ExecutionException e) {
            LOGGER.error("Connection validation failed", e);
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
        finally {
            executor.shutdownNow();
        }
    }
}
