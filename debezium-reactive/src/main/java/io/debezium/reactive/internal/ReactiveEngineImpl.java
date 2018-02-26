/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.StopConnectorException;
import io.debezium.reactive.Converter;
import io.debezium.reactive.DebeziumEvent;
import io.debezium.reactive.ReactiveEngine;
import io.reactivex.Emitter;
import io.reactivex.Flowable;

/**
 * Default implementation of reactive engine.
 *
 * @author Jiri Pechanec
 *
 */
class ReactiveEngineImpl<T> implements ReactiveEngine<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveEngineImpl.class);

    private final RowSendingEmbeddedEngine engine;
    private Consumer<SourceRecord> emitterConsumer; 
    private DebeziumEventImpl<T> currentEvent;
    private Function<SourceRecord, T> serializer;

    private static class DebeziumEventImpl<T> implements DebeziumEvent<T> {

        private final T value;
        private final SourceRecord sourceRecord;
        private boolean completed = false;

        private DebeziumEventImpl(T value, SourceRecord sourceRecord) {
            this.value = value;
            this.sourceRecord = sourceRecord;
        }

        
        public T getRecord() {
            return value;
        }

        @Override
        public void complete() {
            completed = true;
        }

        private boolean isCompleted() {
            return completed;
        }

        private SourceRecord getSourceRecord() {
            return sourceRecord;
        }
    }

    ReactiveEngineImpl(ReactiveEngineBuilderImpl<T> builder) {
        this.engine = new RowSendingEmbeddedEngine(builder.getConfig(), builder.getClassLoader(), builder.getClock(),
                x -> emitterConsumer.accept(x), null, null, builder.getOffsetCommitPolicy());
        @SuppressWarnings("unchecked")

        Function<SourceRecord, T> valueConverter = (x) -> (T)x;
        try {
            if (builder.getConverter() != null) {
                final Converter<T> c = builder.getConverter().newInstance();
                c.configure(builder.getConfig().asMap());
                valueConverter = c::fromConnectData;
            }
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new ConnectException("Could not instantiate serializer " + builder.getConverter().getName());
        }
        this.serializer = valueConverter;
        LOGGER.info("Created a reactive engine from embedded engine '{}'", engine);
    }

    /**
     * Translates events from embedded engine into a {@link Flowable} of change events. Each event
     * must be committed after its processing. If the event is not committed then an error is raised
     * when the consumer tries to read a new event and the previous one is not committed.
     */
    @Override
    public Flowable<DebeziumEvent<T>> stream() {
       return Flowable
                .generate(() -> {
                        engine.doStart();
                        currentEvent = null;
                        LOGGER.info("Consumer subscribed to reactive engine");
                        return engine;
                    }, (RowSendingEmbeddedEngine engine, Emitter<DebeziumEvent<T>> emitter) -> {
                        if (currentEvent != null) {
                            emitter.onError(new IllegalStateException("An uncommitted event exists. Always commit() the event after processing."));
                            return;
                        }
                        emitterConsumer = record -> {
                            LOGGER.trace("Sending record '{}' to the pipeline", record);
                            currentEvent = new DebeziumEventImpl<T>(serializer.apply(record), record);
                            emitter.onNext(currentEvent);
                        };
                        try {
                            engine.run();
                            if (currentEvent == null) {
                                if (Thread.interrupted()) {
                                  LOGGER.debug("Reactive engine interrupted on thread " + Thread.currentThread()
                                  + " while polling the task for records");
                                  emitter.onComplete();
                                }
                            }
                            else if (!currentEvent.isCompleted()) {
                                emitter.onError(new IllegalStateException("An event was not committed after it was processed by pipeline. The pipeline processing has failed or method commit() was not called."));
                            }
                            else {
                                engine.commitRecord(currentEvent.getSourceRecord());
                                currentEvent = null;
                                LOGGER.trace("Pipeline processing finished susccessfully");
                            }
                        }
                        catch (StopConnectorException e) {
                            // Connector stop requested
                            LOGGER.debug("Reactive engine stops due to connector stop");
                            emitter.onComplete();
                        }
                        catch (Throwable t) {
                            LOGGER.error("An exception was thrown from the pipeline", t);
                            emitter.onError(t);
                        }
                    }, (engine) -> {
                        engine.doStopTask();
                        engine.doStopConnector();
                        LOGGER.info("Consumer unsubscribed from reactive engine");
                    });
        
    }
}