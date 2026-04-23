/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.DebeziumEngine.ShutdownStrategy;

public class DebeziumShutdown<R> {
    private final ShutdownStrategy<R> before;
    private final ShutdownStrategy<R> after;

    public DebeziumShutdown(ShutdownStrategy<R> before,
                            ShutdownStrategy<R> after) {
        this.before = before;
        this.after = after;
    }

    public ShutdownStrategy<R> before() {
        return before;
    }

    public ShutdownStrategy<R> after() {
        return after;
    }

    public static <R> DebeziumShutdown<SourceRecord> shutdownAfterProcessing(ShutdownStrategy<R> shutdownStrategy,
                                                                             Function<SourceRecord, R> recordConverter) {
        if (recordConverter == null) {
            return new DebeziumShutdown<>(null, (ShutdownStrategy<SourceRecord>) shutdownStrategy);
        }
        return new DebeziumShutdown<>(null, record -> shutdownStrategy.test(recordConverter.apply(record)));
    }

    public static <R> DebeziumShutdown<R> countdownAfterProcessing(int number) {
        return new DebeziumShutdown<>(null, new CountDown<>(number));
    }

    public static <R> DebeziumShutdown<SourceRecord> shutdownBeforeProcessing(ShutdownStrategy<R> shutdownStrategy,
                                                                              Function<SourceRecord, R> recordConverter) {
        if (shutdownStrategy == null) {
            throw new IllegalArgumentException("shutdown strategy is null");
        }
        if (recordConverter == null) {
            return new DebeziumShutdown<>((ShutdownStrategy<SourceRecord>) shutdownStrategy, null);
        }
        return new DebeziumShutdown<>(record -> shutdownStrategy.test(recordConverter.apply(record)), null);
    }

    public static <R> DebeziumShutdown<R> countdownBeforeProcessing(int number) {
        if (number == 0) {
            throw new IllegalArgumentException("the number of events must be greater than 0");
        }
        return new DebeziumShutdown<>(new CountDown<>(number), null);
    }

    static class CountDown<R> implements ShutdownStrategy<R> {
        private final AtomicInteger counter;

        CountDown(int number) {
            counter = new AtomicInteger(number);
        }

        @Override
        public boolean test(R record) {
            return counter.decrementAndGet() == 0;
        }
    }
}
