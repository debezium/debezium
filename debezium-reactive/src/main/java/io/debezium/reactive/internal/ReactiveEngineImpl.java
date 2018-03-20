/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.StopConnectorException;
import io.debezium.reactive.DataChangeEvent;
import io.debezium.reactive.ReactiveEngine;
import io.debezium.reactive.internal.converters.ConverterRegistry;
import io.debezium.reactive.spi.Converter;
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
    private final Converter<T> converter;
    private Consumer<SourceRecord> emitterConsumer; 
    private DebeziumEventImpl currentEvent;

    private class DebeziumEventImpl implements DataChangeEvent<T> {

        private T key;
        private T value;
        private final SourceRecord sourceRecord;
        private boolean completed = false;
        private boolean keyConverted = false;
        private boolean valueConverted = false;

        private DebeziumEventImpl(SourceRecord sourceRecord) {
            this.sourceRecord = sourceRecord;
        }

        @Override
        public T getKey() {
            if (!keyConverted) {
                key = converter.convertKey(sourceRecord);
            }
            return key;
        }

        @Override
        public T getValue() {
            if (!valueConverted) {
                value = converter.convertValue(sourceRecord);
            }
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

    @SuppressWarnings("unchecked")
    ReactiveEngineImpl(ReactiveEngineBuilderImpl<T> builder) {
        this.engine = new RowSendingEmbeddedEngine(builder.getConfig(), builder.getClassLoader(), builder.getClock(),
                x -> emitterConsumer.accept(x), null, null, builder.getOffsetCommitPolicy());

        final Map<String, String> configs = builder.getConfig().asMap();
        final ConverterRegistry registry = new ConverterRegistry(configs);

        converter = (Converter<T>) registry.getConverter(builder.getAsType());

        LOGGER.info("Created a reactive engine from embedded engine '{}'", engine);
    }

    /**
     * Translates events from embedded engine into a {@link Flowable} of change events. Each event
     * must be committed after its processing. If the event is not committed then an error is raised
     * when the consumer tries to read a new event and the previous one is not committed.
     */
    @Override
    public Flowable<DataChangeEvent<T>> stream() {
       return Flowable
                .generate(() -> {
                        engine.doStart();
                        currentEvent = null;
                        LOGGER.info("Consumer subscribed to reactive engine");
                        return engine;
                    }, (RowSendingEmbeddedEngine engine, Emitter<DataChangeEvent<T>> emitter) -> {
                        if (currentEvent != null) {
                            emitter.onError(new IllegalStateException("An uncommitted event exists. Always commit() the event after processing."));
                            return;
                        }
                        emitterConsumer = record -> {
                            LOGGER.trace("Sending record '{}' to the pipeline", record);
                            currentEvent = new DebeziumEventImpl(record);
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