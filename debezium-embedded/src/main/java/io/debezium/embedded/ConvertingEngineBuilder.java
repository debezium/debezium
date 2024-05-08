/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.HeaderConverter;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Builder;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;

/**
 * A builder that creates a decorator around {@link EmbeddedEngine} that is responsible for the conversion
 * to the final format.
 *
 * @author Jiri Pechanec
 */
public class ConvertingEngineBuilder<R> implements Builder<R> {

    private final Builder<SourceRecord> delegate;
    private final ConverterBuilder converterBuilder;

    private Function<SourceRecord, R> toFormat;
    private Function<R, SourceRecord> fromFormat;

    ConvertingEngineBuilder(ChangeEventFormat<?> format) {
        this(KeyValueHeaderChangeEventFormat.of(null, format.getValueFormat(), null));
    }

    ConvertingEngineBuilder(KeyValueChangeEventFormat<?, ?> format) {
        this(format instanceof KeyValueHeaderChangeEventFormat ? (KeyValueHeaderChangeEventFormat) format
                : KeyValueHeaderChangeEventFormat.of(format.getKeyFormat(), format.getValueFormat(), null));
    }

    ConvertingEngineBuilder(KeyValueHeaderChangeEventFormat<?, ?, ?> format) {
        this.delegate = new EmbeddedEngine.EngineBuilder();
        this.converterBuilder = new ConverterBuilder();
        this.converterBuilder.using(format);
    }

    @Override
    public Builder<R> notifying(Consumer<R> consumer) {
        delegate.notifying((record) -> consumer.accept(toFormat.apply(record)));
        return this;
    }

    private class ConvertingChangeConsumer implements ChangeConsumer<SourceRecord> {

        private final ChangeConsumer<R> handler;

        private ConvertingChangeConsumer(ChangeConsumer<R> handler) {
            this.handler = handler;
        }

        @Override
        public void handleBatch(List<SourceRecord> records, RecordCommitter<SourceRecord> committer) throws InterruptedException {
            handler.handleBatch(records.stream()
                    .map(x -> toFormat.apply(x))
                    .collect(Collectors.toList()),
                    new RecordCommitter<R>() {

                        @Override
                        public void markProcessed(R record) throws InterruptedException {
                            committer.markProcessed(fromFormat.apply(record));
                        }

                        @Override
                        public void markBatchFinished() throws InterruptedException {
                            committer.markBatchFinished();
                        }

                        @Override
                        public void markProcessed(R record, DebeziumEngine.Offsets sourceOffsets)
                                throws InterruptedException {
                            committer.markProcessed(fromFormat.apply(record), sourceOffsets);
                        }

                        @Override
                        public DebeziumEngine.Offsets buildOffsets() {
                            return committer.buildOffsets();
                        }
                    });
        }

        @Override
        public boolean supportsTombstoneEvents() {
            return handler.supportsTombstoneEvents();
        }
    }

    @Override
    public Builder<R> notifying(ChangeConsumer<R> handler) {
        delegate.notifying(new ConvertingChangeConsumer(handler));
        return this;
    }

    @Override
    public Builder<R> using(Properties config) {
        converterBuilder.using(config);
        delegate.using(config);
        return this;
    }

    @Override
    public Builder<R> using(ClassLoader classLoader) {
        delegate.using(classLoader);
        return this;
    }

    @Override
    public Builder<R> using(Clock clock) {
        delegate.using(clock);
        return this;
    }

    @Override
    public Builder<R> using(CompletionCallback completionCallback) {
        delegate.using(completionCallback);
        return this;
    }

    @Override
    public Builder<R> using(ConnectorCallback connectorCallback) {
        delegate.using(connectorCallback);
        return this;
    }

    @Override
    public Builder<R> using(OffsetCommitPolicy policy) {
        delegate.using(policy);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumEngine<R> build() {
        final DebeziumEngine<SourceRecord> engine = delegate.build();
        HeaderConverter headerConverter = converterBuilder.headerConverter();
        toFormat = converterBuilder.toFormat(headerConverter);
        fromFormat = converterBuilder.fromFormat();
        return new DebeziumEngine<R>() {

            @Override
            public void run() {
                engine.run();
            }

            @Override
            public void close() throws IOException {
                if (headerConverter != null) {
                    headerConverter.close();
                }
                engine.close();
            }
        };
    }
}
