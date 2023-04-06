/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;

public class ErrorHandler {

    public static final int RETRIES_UNLIMITED = -1;
    public static final int RETRIES_DISABLED = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

    private final ChangeEventQueue<?> queue;
    private final AtomicReference<Throwable> producerThrowable;
    private final CommonConnectorConfig connectorConfig;
    private int maxRetries;
    private int retries;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                        ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.producerThrowable = new AtomicReference<>();
        if (connectorConfig != null) {
            this.maxRetries = connectorConfig.getMaxRetriesOnError();
        }
        else {
            this.maxRetries = RETRIES_UNLIMITED;
        }
        if (replacedErrorHandler != null) {
            this.retries = replacedErrorHandler.getRetries();
        }
    }

    public ErrorHandler(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                        ChangeEventQueue<?> queue) {
        this(connectorType, connectorConfig, queue, null);
    }

    public void setProducerThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);

        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);
        boolean retriable = isRetriable(producerThrowable);

        if (!retriable) {
            retriable = isCustomRetriable(producerThrowable);
        }

        if (first) {
            if (retriable && hasMoreRetries()) {
                queue.producerException(
                        new RetriableException("An exception occurred in the change event producer. This connector will be restarted.", producerThrowable));
            }
            else {
                queue.producerException(new ConnectException("An exception occurred in the change event producer. This connector will be stopped.", producerThrowable));
            }
        }
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }

    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collections.singleton(IOException.class);
    }

    /**
     * Whether the given throwable is retriable (e.g. an exception indicating a
     * connection loss) or not.
     * By default only I/O exceptions are retriable
     */
    protected boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        for (Class<? extends Exception> e : communicationExceptions()) {
            if (e.isAssignableFrom(throwable.getClass())) {
                return true;
            }
        }
        return isRetriable(throwable.getCause());
    }

    /**
     * Whether the given non-retriable matches a custom retriable setting.
     *
     * @return true if non-retriable is converted to retriable
     */
    protected boolean isCustomRetriable(Throwable throwable) {
        if (!connectorConfig.customRetriableException().isPresent()) {
            return false;
        }
        while (throwable != null) {
            if (throwable.getMessage() != null
                    && throwable.getMessage().matches(connectorConfig.customRetriableException().get())) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    /**
     * Whether the maximum number of retries has been reached
     *
     * @return true if maxRetries is -1 or retries < maxRetries
     */
    protected boolean hasMoreRetries() {
        boolean doRetry = unlimitedRetries() || retries < maxRetries;
        if (doRetry) {
            retries++;
            LOGGER.warn("Retry {} of {} retries will be attempted", retries,
                    unlimitedRetries() ? "unlimited" : maxRetries);
        }
        else {
            LOGGER.error("The maximum number of {} retries has been attempted", maxRetries);
        }

        return doRetry;
    }

    private boolean unlimitedRetries() {
        return maxRetries == RETRIES_UNLIMITED;
    }

    public int getRetries() {
        return retries;
    }

    public void resetRetries() {
        this.retries = 0;
    }
}
