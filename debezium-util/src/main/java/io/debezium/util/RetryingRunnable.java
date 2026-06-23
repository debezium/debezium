/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.ThrowingRunnable;

/**
 * Allows to re-try a runnable action if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between retries is defined by {@link DelayStrategy}, which needs to be provided by the implementing class.
 * Optionally, an auto-heal action can be provided, which is executed before each retry.
 * Inspired by: io.debezium.embedded.async.RetryingCallable.
 */
public class RetryingRunnable<E extends Exception> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingRunnable.class);

    private final int retries;
    private final ThrowingRunnable<E> doRun;
    private final ThrowingRunnable<E> doAutoHeal;
    private final DelayStrategy delayStrategy;

    // package-private / private: construction goes through the builder
    private RetryingRunnable(Builder<E> b) {
        this.retries = b.retries;
        this.doRun = b.doRun;
        this.doAutoHeal = b.doAutoHeal;
        this.delayStrategy = b.delayStrategy;
    }

    public static <E extends Exception> Builder<E> builder() {
        return new Builder<>();
    }

    public void runWrapped(Function<Throwable, E> exceptionWrapper) throws E {
        try {
            run();
        }
        catch (InterruptedException ex) {
            throw exceptionWrapper.apply(ex);
        }
    }

    public void run() throws E, InterruptedException {
        // 0 retries means retries are disabled,
        // -1 means infinite retries; int range is not infinite, but in this case probably a sufficient approximation.
        // We start from `retries` as the last call attempt is done out of the retry loop and this last call either
        // succeeds or throws an exception which is propagated further. I.e. the actual number of calls is `retries+1`,
        // meaning one ordinary call and #`retries` is it fails.
        int attempts = retries;
        while (attempts != 0) {
            try {
                doRun.run();
                return;
            }
            catch (InterruptedException ex) {
                throw ex;
            }
            catch (Exception ex) {
                // This must be E or a RuntimeException
                attempts--;
                String retriesExplained = retries == -1 ? "infinity" : String.valueOf(retries);
                LOGGER.info("Runnable failed with exception, will try and auto heal; attempt #{} out of {}",
                        retries - attempts,
                        retriesExplained,
                        ex);

                // Auto heal
                try {
                    doAutoHeal.run();
                }
                catch (InterruptedException exAutoHeal) {
                    throw exAutoHeal;
                }
                catch (Exception exAutoHeal) {
                    // Again, this must be E or a RuntimeException
                    LOGGER.info("Auto heal failed with exception, will retry later; attempt #{} out of {}",
                            retries - attempts,
                            retriesExplained,
                            exAutoHeal);
                    delayStrategy.sleepWhen(true);
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Runnable was interrupted while sleeping in DelayStrategy");
                    }
                }
            }
        }
        doRun.run();
    }

    // ---- Builder ----
    public static final class Builder<E extends Exception> {
        private int retries = 0;
        private ThrowingRunnable<E> doRun;
        private ThrowingRunnable<E> doAutoHeal = () -> {
        }; // default: no-op heal
        private DelayStrategy delayStrategy = DelayStrategy.none(); // default: no delay

        private Builder() {
        }

        public Builder<E> retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder<E> doRun(ThrowingRunnable<E> doRun) {
            this.doRun = doRun;
            return this;
        }

        public Builder<E> doAutoHeal(ThrowingRunnable<E> doAutoHeal) {
            this.doAutoHeal = doAutoHeal;
            return this;
        }

        public Builder<E> delayStrategy(DelayStrategy delayStrategy) {
            this.delayStrategy = delayStrategy;
            return this;
        }

        public RetryingRunnable<E> build() {
            if (doRun == null) {
                throw new IllegalStateException("doRun must be provided");
            }
            // delayStrategy / doAutoHeal have sensible defaults above
            return new RetryingRunnable<>(this);
        }
    }
}
