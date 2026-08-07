/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.concurrent.Callable;

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.DelayStrategy;
import io.debezium.util.RetryingSupplier;

/**
 * Extension to {@link Callable}, which allows to re-try the action if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between retries is defined by {@link DelayStrategy}, which needs to be provided by the implementing class.
 * The action is re-tried when a {@link RetriableException} is thrown, either directly or as any exception in the
 * cause chain of the thrown exception. The retry loop is implemented by {@link RetryingSupplier}, to which this
 * class delegates.
 *
 * @author vjuranek
 */
public abstract class RetryingCallable<V> implements Callable<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingCallable.class);

    private final int retries;

    public RetryingCallable(final int retries) {
        this.retries = retries;
    }

    public abstract V doCall() throws Exception;

    public abstract DelayStrategy delayStrategy();

    public V call() throws Exception {
        return RetryingSupplier.<V, Exception> builder()
                .retries(retries)
                .doGet(this::doCall)
                .retriableExceptions(RetriableException.class)
                .delayStrategy(delayStrategy())
                .name("Callable")
                .logger(LOGGER)
                .build()
                .get();
    }
}
