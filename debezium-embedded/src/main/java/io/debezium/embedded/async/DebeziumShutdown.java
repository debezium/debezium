/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.engine.DebeziumEngine.Shutdown;
import io.debezium.engine.DebeziumEngine.ShutdownStrategy;

/**
 * Default implementation of {@link Shutdown} that pairs two optional {@link ShutdownStrategy strategies},
 * one evaluated before and one after each record is consumed.
 *
 * <p>Instances are created through the nested {@link Builder}:
 * <pre>{@code
 * Shutdown<R> shutdown = new DebeziumShutdown.Builder<R>()
 *         .after().records(100)
 *         .build();
 * }</pre>
 *
 * @param <R> the record type produced by the engine
 * @see io.debezium.engine.DebeziumEngine.Shutdown
 * @see io.debezium.engine.DebeziumEngine.ShutdownStrategy
 */
public class DebeziumShutdown<R> implements Shutdown<R> {

    private final ShutdownStrategy<ShutdownContext<R>> before;
    private final ShutdownStrategy<ShutdownContext<R>> after;

    /**
     * Creates a new instance from the values accumulated in the given builder.
     *
     * @param builder the builder; never null
     */
    private DebeziumShutdown(DebeziumShutdown.Builder<R> builder) {
        this.before = builder.before;
        this.after = builder.after;
    }

    @Override
    public ShutdownStrategy<ShutdownContext<R>> before() {
        return before;
    }

    @Override
    public ShutdownStrategy<ShutdownContext<R>> after() {
        return after;
    }

    /**
     * Fluent builder for {@link DebeziumShutdown} instances.
     *
     * <p>Call {@link #before()} or {@link #after()} to obtain a {@link ConsumingBuilder} that lets you
     * choose a built-in strategy (e.g. {@link ConsumingBuilder#records(int)}) or supply a
     * {@link ConsumingBuilder#custom(ShutdownStrategy) custom} one.
     *
     * @param <R> the record type produced by the engine
     */
    public static class Builder<R> {

        private ShutdownStrategy<ShutdownContext<R>> before;
        private ShutdownStrategy<ShutdownContext<R>> after;

        /**
         * Begins configuration of the post-consumer shutdown strategy.
         *
         * @return a {@link ConsumingBuilder} for selecting the after-consumer strategy
         */
        public ConsumingBuilder<R> after() {
            return new ConsumingBuilder<>() {
                @Override
                public Builder<R> records(int number) {
                    Builder.this.after = new Builder.CountDown<>(number);
                    return Builder.this;
                }

                @Override
                public Builder<R> custom(ShutdownStrategy<ShutdownContext<R>> strategy) {
                    Builder.this.after = strategy;
                    return Builder.this;
                }
            };
        }

        /**
         * Begins configuration of the pre-consumer shutdown strategy.
         *
         * @return a {@link ConsumingBuilder} for selecting the before-consumer strategy
         */
        public ConsumingBuilder<R> before() {
            return new ConsumingBuilder<>() {
                @Override
                public Builder<R> records(int number) {
                    Builder.this.before = new Builder.CountDown<>(number);
                    return Builder.this;
                }

                @Override
                public Builder<R> custom(ShutdownStrategy<ShutdownContext<R>> strategy) {
                    Builder.this.before = strategy;
                    return Builder.this;
                }
            };
        }

        /**
         * Creates a {@link DebeziumShutdown} with the strategies configured on this builder.
         *
         * @return a new {@link Shutdown} instance; never null
         */
        public Shutdown<R> build() {
            return new DebeziumShutdown<>(this);
        }

        /**
         * Secondary builder returned by {@link Builder#before()} and {@link Builder#after()} that
         * selects the concrete shutdown strategy.
         *
         * @param <R> the record type produced by the engine
         */
        public interface ConsumingBuilder<R> {

            /**
             * Shuts down the engine after the given number of records have been evaluated.
             *
             * @param number the number of records to process before triggering shutdown; must be positive
             * @return the parent builder so the call chain can continue
             */
            Builder<R> records(int number);

            /**
             * Shuts down the engine using a custom strategy.
             *
             * @param strategy the strategy to apply; never null
             * @return the parent builder so the call chain can continue
             */
            Builder<R> custom(ShutdownStrategy<ShutdownContext<R>> strategy);
        }

        /**
         * A {@link ShutdownStrategy} that triggers shutdown after a fixed number of records have been evaluated.
         * The counter is decremented atomically on each call to {@link #test(Object)}.
         *
         * @param <R> the context type passed to the predicate
         */
        private static class CountDown<R> implements ShutdownStrategy<R> {
            private final AtomicInteger counter;

            private CountDown(int number) {
                counter = new AtomicInteger(number);
            }

            @Override
            public boolean test(R record) {
                return counter.decrementAndGet() == 0;
            }
        }
    }

}
