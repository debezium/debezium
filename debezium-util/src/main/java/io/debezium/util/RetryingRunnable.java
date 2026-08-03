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
 * The delay between attempts is defined by {@link DelayStrategy}.
 * Optionally, an auto-heal action can be provided, which is executed before each retry: when it succeeds the action
 * is retried immediately, when it fails (or when no auto-heal is configured) the delay strategy is applied.
 * Optionally, a list of retriable exception types can be provided: if the list is empty, the action is retried for
 * all exceptions, otherwise it is retried only for exceptions which are instances of one of the supplied types
 * (i.e. the supplied type or any of its descendants). A non-retriable exception is propagated immediately.
 * The retry loop is implemented by {@link RetryingSupplier}, to which this class delegates.
 */
public class RetryingRunnable<E extends Exception> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingRunnable.class);

    private final RetryingSupplier<Void, E> delegate;

    private RetryingRunnable(Builder<E> b) {
        this.delegate = RetryingSupplier.<Void, E> builder()
                .retries(b.retries)
                .doGet(() -> {
                    b.doRun.run();
                    return null;
                })
                .doAutoHeal(b.doAutoHeal)
                .delayStrategy(b.delayStrategy)
                .retriableExceptions(b.retriableExceptions)
                .name("Runnable")
                .logger(LOGGER)
                .build();
    }

    public static <E extends Exception> Builder<E> builder() {
        return new Builder<>();
    }

    public void runWrapped(Function<Throwable, E> exceptionWrapper) throws E {
        delegate.getWrapped(exceptionWrapper);
    }

    public void run() throws E, InterruptedException {
        delegate.get();
    }

    public static final class Builder<E extends Exception> {
        private int retries = 0;
        private ThrowingRunnable<E> doRun;
        private ThrowingRunnable<E> doAutoHeal;
        private DelayStrategy delayStrategy = DelayStrategy.none();
        private List<Class<? extends Exception>> retriableExceptions = new ArrayList<>();

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
            return new RetryingRunnable<>(this);
        }
    }
}
