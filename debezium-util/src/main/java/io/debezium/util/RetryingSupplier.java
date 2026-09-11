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
import io.debezium.function.ThrowingSupplier;

/**
 * Allows to re-try an action producing a value if exception is thrown during the execution.
 * The action is re-tried {@code retries} number of times.
 * The delay between attempts is defined by {@link DelayStrategy}.
 * Optionally, an auto-heal action can be provided, which is executed before each retry: when it succeeds the action
 * is retried immediately, when it fails (or when no auto-heal is configured) the delay strategy is applied.
 * Optionally, a list of retriable exception types can be provided: if the list is empty, the action is retried for
 * all exceptions, otherwise it is retried only for exceptions which are, or contain in their cause chain, an
 * instance of one of the supplied types. A non-retriable exception is propagated immediately.
 * Log messages are emitted through the configurable logger, so delegating adapters keep their own log category.
 * This class hosts the retry loop shared with {@link RetryingRunnable}.
 */
public class RetryingSupplier<V, E extends Exception> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingSupplier.class);

    private final int retries;
    private final ThrowingSupplier<V, E> doGet;
    private final ThrowingRunnable<E> doAutoHeal;
    private final DelayStrategy delayStrategy;
    private final List<Class<? extends Exception>> retriableExceptions;
    private final String customRetriableMessagePattern;
    private final boolean walkCauseChain;
    private final String name;
    private final Logger logger;

    private RetryingSupplier(Builder<V, E> b) {
        this.retries = b.retries;
        this.doGet = b.doGet;
        this.doAutoHeal = b.doAutoHeal;
        this.delayStrategy = b.delayStrategy;
        this.retriableExceptions = b.retriableExceptions;
        this.customRetriableMessagePattern = b.customRetriableMessagePattern;
        this.walkCauseChain = b.walkCauseChain;
        this.name = b.name;
        this.logger = b.logger;
    }

    public static <V, E extends Exception> Builder<V, E> builder() {
        return new Builder<>();
    }

    public V getWrapped(Function<Throwable, E> exceptionWrapper) throws E {
        try {
            return get();
        }
        catch (InterruptedException ex) {
            throw exceptionWrapper.apply(ex);
        }
    }

    public V get() throws E, InterruptedException {
        // 0 retries means retries are disabled,
        // -1 means infinite retries; int range is not infinite, but in this case probably a sufficient approximation.
        // We start from `retries` as the last call attempt is done out of the retry loop and this last call either
        // succeeds or throws an exception which is propagated further. I.e. the actual number of calls is `retries+1`,
        // meaning one ordinary call and #`retries` is it fails.
        int attempts = retries;
        while (attempts != 0) {
            try {
                final V result = doGet.get();
                if (attempts != retries) {
                    logger.debug("{} succeeded after {} retry attempt(s)", name, retries - attempts);
                }
                return result;
            }
            catch (InterruptedException ex) {
                throw ex;
            }
            catch (Exception ex) {
                if (!isRetriable(ex)) {
                    throwAsEOrRuntime(ex);
                }
                attempts--;
                String retriesExplained = retries == -1 ? "infinity" : String.valueOf(retries);
                logger.info("{} failed with exception, will try and auto heal (if configured); attempt #{} out of {}",
                        name,
                        retries - attempts,
                        retriesExplained,
                        ex);

                if (doAutoHeal == null) {
                    executeDelayStrategy();
                }
                else {
                    try {
                        doAutoHeal.run();
                    }
                    catch (InterruptedException exAutoHeal) {
                        throw exAutoHeal;
                    }
                    catch (Exception exAutoHeal) {
                        logger.info("Auto heal of {} failed with exception, will retry later; attempt #{} out of {}",
                                name,
                                retries - attempts,
                                retriesExplained,
                                exAutoHeal);
                        executeDelayStrategy();
                    }
                }
            }
        }
        final V result = doGet.get();
        if (retries > 0) {
            logger.debug("{} succeeded after {} retry attempt(s)", name, retries);
        }
        return result;
    }

    private void executeDelayStrategy() throws InterruptedException {
        delayStrategy.sleepWhen(true);
        // DelayStrategy catches interrupted exception during the sleep and just set back interrupted status.
        // We need to re-throw the InterruptedException to avoid unwanted cycles in the retry loop, e.g. when
        // executor service running this action shuts down. Without re-throwing the exception it would
        // result into cycling in the retry loop without any sleep in DelayStrategy until the running thread is
        // killed by the executor service.
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException(name + " was interrupted while sleeping in DelayStrategy");
        }
    }

    /**
     * Returns {@code true} if the given exception should be retried. When no retriable exception types were
     * configured (empty list), every exception is retriable. Otherwise the exception is retriable only if it, or
     * any exception in its cause chain, is an instance of one of the configured types (or a descendant thereof).
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
            if (customRetriableMessagePattern != null && current.getMessage() != null
                    && current.getMessage().matches(customRetriableMessagePattern)) {
                return true;
            }
            if (!walkCauseChain) {
                return false;
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
     * Re-throws the given exception. Inside {@link #get()} a caught {@link Exception} must be either {@code E} or a
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

    public static final class Builder<V, E extends Exception> {
        private int retries = 0;
        private ThrowingSupplier<V, E> doGet;
        private ThrowingRunnable<E> doAutoHeal;
        private DelayStrategy delayStrategy = DelayStrategy.none();
        private List<Class<? extends Exception>> retriableExceptions = new ArrayList<>();
        private String customRetriableMessagePattern;
        private boolean walkCauseChain = true;
        private String name = "Operation";
        private Logger logger = LOGGER;

        private Builder() {
        }

        public Builder<V, E> retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder<V, E> doGet(ThrowingSupplier<V, E> doGet) {
            this.doGet = doGet;
            return this;
        }

        public Builder<V, E> doAutoHeal(ThrowingRunnable<E> doAutoHeal) {
            this.doAutoHeal = doAutoHeal;
            return this;
        }

        public Builder<V, E> delayStrategy(DelayStrategy delayStrategy) {
            this.delayStrategy = delayStrategy;
            return this;
        }

        /**
         * Sets the name used in log messages to identify the retried operation.
         */
        public Builder<V, E> name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the logger used for retry messages, so that delegating adapters keep their own log category.
         */
        public Builder<V, E> logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * Sets the list of retriable exception types. If empty (the default), all exceptions are retried;
         * otherwise only exceptions assignable to one of the supplied types are retried. A {@code null} argument
         * is treated as an empty list (retry all).
         */
        public Builder<V, E> retriableExceptions(List<Class<? extends Exception>> retriableExceptions) {
            this.retriableExceptions = (retriableExceptions == null)
                    ? new ArrayList<>()
                    : new ArrayList<>(retriableExceptions);
            return this;
        }

        /**
         * Sets a regular expression matched against the messages of the thrown exception and, subject to
         * {@link #walkCauseChain(boolean)}, its causes; a match classifies the exception as retriable even
         * when its type does not. This mirrors the semantics of the internal
         * {@code custom.retriable.exception} connector option. A {@code null} pattern (the default)
         * disables the message-based classification.
         */
        public Builder<V, E> customRetriableMessagePattern(String customRetriableMessagePattern) {
            this.customRetriableMessagePattern = customRetriableMessagePattern;
            return this;
        }

        /**
         * Whether the retriability classification walks the exception cause chain (the default) or
         * examines the thrown exception alone. Callers whose failure contract distinguishes a
         * retriable exception from a terminal one that merely wraps it, such as the engine polling
         * loop where {@code ChangeEventQueue} raises a {@code ConnectException} to stop the task,
         * should disable the walk to keep that distinction.
         */
        public Builder<V, E> walkCauseChain(boolean walkCauseChain) {
            this.walkCauseChain = walkCauseChain;
            return this;
        }

        @SafeVarargs
        public final Builder<V, E> retriableExceptions(Class<? extends Exception>... retriableExceptions) {
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

        public RetryingSupplier<V, E> build() {
            if (doGet == null) {
                throw new IllegalStateException("doGet must be provided");
            }
            return new RetryingSupplier<>(this);
        }
    }
}
