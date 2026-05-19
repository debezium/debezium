package io.debezium.embedded.async;

import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.engine.DebeziumEngine.Shutdown;
import io.debezium.engine.DebeziumEngine.ShutdownStrategy;

public class DebeziumShutdown<R> implements Shutdown<R> {

    private final ShutdownStrategy<ShutdownContext<R>> before;
    private final ShutdownStrategy<ShutdownContext<R>> after;

    public DebeziumShutdown(DebeziumShutdown.Builder<R> builder) {
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

    public static class Builder<R> {

        private ShutdownStrategy<ShutdownContext<R>> before;
        private ShutdownStrategy<ShutdownContext<R>> after;

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

        public Shutdown<R> build() {
            return new DebeziumShutdown<>(this);
        }

        public interface ConsumingBuilder<R> {
            Builder<R> records(int number);

            Builder<R> custom(ShutdownStrategy<ShutdownContext<R>> strategy);
        };

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
