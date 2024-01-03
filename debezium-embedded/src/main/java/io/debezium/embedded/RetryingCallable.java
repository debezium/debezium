/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.Callable;

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.DelayStrategy;

/**
 * Extension to {@link Callable}, which allows to re-try the action if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between retries is defined by {@link DelayStrategy}, which needs to be provided by the implementing class.
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
        final DelayStrategy delayStrategy = delayStrategy();
        // 0 retries means retries are disabled,
        // -1 means infinite retries; int range is not infinite, but in this case probably a sufficient approximation.
        // We start from retries-1 as the last call attempt is done out of the retry loop and this last call either
        // succeeds or throws an exception which is propagated further.
        int attempts = retries;
        while (attempts != 0) {
            try {
                return doCall();
            }
            catch (RetriableException e) {
                attempts--;
                String retriesExplained = retries == -1 ? "infinity" : String.valueOf(retries);
                LOGGER.info("Failed with retriable exception, will retry later; attempt #{} out of {}",
                        retries - attempts,
                        retriesExplained,
                        e);
                delayStrategy.sleepWhen(true);
                // DelayStrategy catches interrupted exception during the sleep and just set back interrupted status.
                // We need to re-throw the InterruptedException to avoid unwanted cycles in the retry loop, e.g. when
                // executor service running this callable shuts down. Without re-throwing the exception it would
                // result into cycling in the retry loop without any sleep in DelayStrategy until the running thread is
                // killed by the executor service.
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Callable was interrupted while sleeping in DelayStrategy");
                }
            }
        }
        return doCall();
    }
}
