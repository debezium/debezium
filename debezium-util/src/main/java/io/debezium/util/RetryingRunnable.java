/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.ThrowingRunnable;

/**
 * Allows to re-try a runnable action if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between retries is defined by {@link DelayStrategy}, which needs to be provided by the implementing class.
 * Optionally, an auto-heal action can be provided, which is executed before each retry.
 * Optionally, a list of retriable exception types can be provided: if the list is empty, the action is retried for
 * all exceptions, otherwise it is retried only for exceptions which are instances of one of the supplied types
 * (i.e. the supplied type or any of its descendants). A non-retriable exception is propagated immediately.
 * Inspired by: io.debezium.embedded.async.RetryingCallable.
 */
public class RetryingRunnable<E extends Exception> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingRunnable.class);

    private final int retries;
    private final ThrowingRunnable<E> doRun;
    private final ThrowingRunnable<E> doAutoHeal;
    private final DelayStrategy delayStrategy;
    private final List<Class<? extends Exception>> retriableExceptions;

    // package-private / private: construction goes through the builder
    private RetryingRunnable(Builder<E> b) {
        this.retries = b.retries;
        this.doRun = b.doRun;
        this.doAutoHeal = b.doAutoHeal;
        this.delayStrategy = b.delayStrategy;
        this.retriableExceptions = b.retriableExceptions;
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
                if (!isRetriable(ex)) {
                    // Not in the retriable list: propagate immediately without auto heal or delay.
                    throwAsEOrRuntime(ex);
                }
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

    /**
     * Returns {@code true} if the given exception should be retried. When no retriable exception types were
     * configured (empty list), every exception is retriable. Otherwise the exception is retriable only if it is
     * an instance of one of the configured types (or a descendant thereof).
     */
    private boolean isRetriable(Exception ex) {
        if (retriableExceptions.isEmpty()) {
            return true;
        }
        Throwable current = ex;
        Throwable slow = ex; // Floyd's cycle guard
        boolean advanceSlow = false;
        while (current != null) {
            for (Class<? extends Exception> retriable : retriableExceptions) {
                if (retriable.isInstance(current)) {
                    return true;
                }
            }
            current = current.getCause();
            if (advanceSlow) {
                slow = slow.getCause();
                if (current == slow) {
                    break; // cycle detected
                }
            }
            advanceSlow = !advanceSlow;
        }
        return false;
    }

    /**
     * Re-throws the given exception. Inside {@link #run()} a caught {@link Exception} must be either {@code E} or a
     * {@link RuntimeException}, so this cast is safe; it lets us propagate a non-retriable exception while keeping
     * the checked {@code throws E} contract.
     */
    @SuppressWarnings("unchecked")
    private void throwAsEOrRuntime(Exception ex) throws E {
        if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        }
        throw (E) ex;
    }

    // ---- Builder ----
    public static final class Builder<E extends Exception> {
        private int retries = 0;
        private ThrowingRunnable<E> doRun;
        private ThrowingRunnable<E> doAutoHeal = () -> {
        }; // default: no-op heal
        private DelayStrategy delayStrategy = DelayStrategy.none(); // default: no delay
        private List<Class<? extends Exception>> retriableExceptions = new ArrayList<>(); // default: retry all

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

        /**
         * Sets the list of retriable exception types. If empty (the default), all exceptions are retried;
         * otherwise only exceptions assignable to one of the supplied types are retried. A {@code null} argument
         * is treated as an empty list (retry all).
         */
        public Builder<E> retriableExceptions(List<Class<? extends Exception>> retriableExceptions) {
            this.retriableExceptions = (retriableExceptions == null)
                    ? new ArrayList<>()
                    : new ArrayList<>(retriableExceptions);
            return this;
        }

        @SafeVarargs
        public final Builder<E> retriableExceptions(Class<? extends Exception>... retriableExceptions) {
            this.retriableExceptions = new ArrayList<>();
            if (retriableExceptions != null) {
                for (Class<? extends Exception> type : retriableExceptions) {
                    if (type != null) {
                        this.retriableExceptions.add(type);
                    }
                }
            }
            return this;
        }

        public RetryingRunnable<E> build() {
            if (doRun == null) {
                throw new IllegalStateException("doRun must be provided");
            }
            // delayStrategy / doAutoHeal / retriableExceptions have sensible defaults above
            return new RetryingRunnable<>(this);
        }
    }
}
