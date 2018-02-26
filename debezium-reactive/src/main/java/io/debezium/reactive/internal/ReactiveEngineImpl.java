/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.StopConnectorException;
import io.debezium.embedded.internal.EmbeddedEngineImpl;
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
class ReactiveEngineImpl implements ReactiveEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveEngineImpl.class);

    private final EmbeddedEngineImpl engine;
    private Consumer<SourceRecord> emitterConsumer; 
    private DebeziumEventImpl currentEvent;

    private static class DebeziumEventImpl implements DebeziumEvent {

        private final SourceRecord record;
        private boolean committed = false;

        private DebeziumEventImpl(SourceRecord record) {
            this.record = record;
        }

        
        public SourceRecord getRecord() {
            return record;
        }

        @Override
        public void commit() {
            committed = true;
        }

        private boolean isCommitted() {
            return committed;
        }
    }

    ReactiveEngineImpl(io.debezium.embedded.EmbeddedEngine.Builder engineBuilder) {
        this.engine = (EmbeddedEngineImpl)engineBuilder.notifying(x -> emitterConsumer.accept(x)).build();
        LOGGER.info("Created a reactive engine from embedded engine '{}'", engine);
    }

    /**
     * Translates events from embedded engine into a {@link Flowable} of change events. Each event
     * must be committed after its processing. If the event is not committed then an error is raised
     * when the consumer tries to read a new event and the previous one is not committed.
     */
    @Override
    public Flowable<DebeziumEvent> flowableWithBackPressure() {
       return Flowable
                .generate(() -> {
                        engine.doStart();
                        currentEvent = null;
                        LOGGER.info("Consumer subscribed to reactive engine");
                        return engine;
                    }, (EmbeddedEngineImpl engine, Emitter<DebeziumEvent> emitter) -> {
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
                            engine.pollSingle();
                            if (!currentEvent.isCommitted()) {
                                emitter.onError(new IllegalStateException("An event was not committed after it was processed by pipeline. The pipeline processing has failed or method commit() was not called."));
                            }
                            else {
                                engine.commitOneRecord(currentEvent.getRecord());
                                currentEvent = null;
                                LOGGER.trace("Pipeline processing finished susccessfully");
                            }
                        }
                        catch (InterruptedException e) {
                            // Interrupted while polling ...
                            LOGGER.debug("Reactive engine interrupted on thread " + Thread.currentThread()
                                    + " while polling the task for records");
                            Thread.interrupted();
                            emitter.onComplete();
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
                        engine.doCompleteTask(null);
                        engine.doCompleteConnector();
                        LOGGER.info("Consumer unsubscribed from reactive engine");
                    });
        
    }
}
