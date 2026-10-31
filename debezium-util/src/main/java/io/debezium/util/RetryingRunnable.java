/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.ThrowingRunnable;

/**
 * Allows to re-try a runnable action if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between retries is defined by {@link DelayStrategy}, which needs to be provided by the implementing class.
 * Optionally, an auto-heal action can be provided, which is executed before each retry.
 * Optionally, retriable conditions can be configured: a list of retriable exception types and/or a
 * {@code retriableWhen} predicate. An exception is retried when any configured condition matches it: when it is an
 * instance of one of the supplied types (or a descendant thereof), or when the predicate returns {@code true} for it.
 * If no condition is configured at all, the action is retried for all exceptions.
 * A non-retriable exception is propagated immediately.
 * Inspired by: io.debezium.embedded.async.RetryingCallable.
 */
public class RetryingRunnable<E extends Exception> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingRunnable.class);

    private final ThrowingRunnable<E> NO_OP_HEAL = () -> {
    };
    private final int retries;
    private final ThrowingRunnable<E> doRun;
    private final ThrowingRunnable<E> doAutoHeal;
    private final DelayStrategy delayStrategy;
    private final List<Class<? extends Exception>> retriableExceptions;
    private final Predicate<Exception> retriableWhen;

    // package-private / private: construction goes through the builder
    private RetryingRunnable(Builder<E> b) {
        this.retries = b.retries;
        this.doRun = b.doRun;
        this.doAutoHeal = b.doAutoHeal == null ? NO_OP_HEAL : b.doAutoHeal;
        this.delayStrategy = b.delayStrategy;
        this.retriableExceptions = b.retriableExceptions;
        this.retriableWhen = b.retriableWhen;
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
                    // Not retriable: propagate immediately without auto heal or delay.
                    throwAsEOrRuntime(ex);
                }
                attempts--;
                String retriesExplained = retries == -1 ? "infinity" : String.valueOf(retries);
                LOGGER.info("Runnable failed with exception, will try and auto heal (if configured); attempt #{} out of {}",
                        retries - attempts,
                        retriesExplained,
                        ex);

                if (doAutoHeal == NO_OP_HEAL) {
                    executeDelayStrategy();
                }
                else {
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
                        executeDelayStrategy();
                    }
                }

            }
        }
        doRun.run();
    }

    private void executeDelayStrategy() throws InterruptedException {
        delayStrategy.sleepWhen(true);
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Runnable was interrupted while sleeping in DelayStrategy");
        }
    }

    /**
     * Returns {@code true} if the given exception should be retried. The exception is retriable when any
     * configured condition matches it: the {@code retriableWhen} predicate returns {@code true} for it, or it
     * is an instance of one of the configured types (or a descendant thereof). When no condition is configured
     * at all, every exception is retriable.
     */
    private boolean isRetriable(Exception ex) {
        if (retriableExceptions.isEmpty() && retriableWhen == null) {
            return true;
        }
        return isRetriableType(ex) || isRetriableWhen(ex);
    }

    private boolean isRetriableWhen(Exception ex) {
        if (retriableWhen == null) {
            return false;
        }
        try {
            return retriableWhen.test(ex);
        }
        catch (RuntimeException predicateException) {
            LOGGER.error("The retriableWhen predicate threw an exception while evaluating {}, treating the original exception as non-retriable",
                    ex.getClass().getName(), predicateException);
            return false;
        }
    }

    /**
     * Returns {@code true} if the given exception or one of its causes is an instance of one of the configured
     * retriable exception types. An empty type list matches nothing; the "retry all" default when no condition
     * is configured is handled by {@link #isRetriable(Exception)}.
     */
    private boolean isRetriableType(Exception ex) {
        if (retriableExceptions.isEmpty()) {
            return false;
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
        private ThrowingRunnable<E> doAutoHeal;
        private DelayStrategy delayStrategy = DelayStrategy.none(); // default: no delay
        private List<Class<? extends Exception>> retriableExceptions = new ArrayList<>(); // default: no type condition
        private Predicate<Exception> retriableWhen; // default: no predicate condition

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
         * Sets the list of retriable exception types. Only exceptions assignable to one of the supplied types
         * (or having such a cause) are retried via this condition. A {@code null} argument is treated
         * as an empty list (no type condition).
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

        /**
         * Sets a predicate deciding whether a given exception should be retried. The predicate receives the
         * exception thrown by the action as-is, i.e. without unwrapping its cause chain. If the predicate itself
         * throws a {@link RuntimeException}, it is caught and logged, and the original exception is treated
         * as non-retriable and propagated as-is. A {@code null} argument is treated as not set
         * (no predicate condition).
         */
        public Builder<E> retriableWhen(Predicate<Exception> retriableWhen) {
            this.retriableWhen = retriableWhen;
            return this;
        }

        public RetryingRunnable<E> build() {
            if (doRun == null) {
                throw new IllegalStateException("doRun must be provided");
            }
            // delayStrategy / doAutoHeal / retriableExceptions / retriableWhen have sensible defaults above
            return new RetryingRunnable<>(this);
        }
    }
}
