/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.embedded.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link RecordProcessor}, which provides implementation of processor initialization, while the record processing implementation
 * left to the children classes.
 */
public abstract class AbstractRecordProcessor<R> implements RecordProcessor<R> {
    protected ExecutorService recordService;
    protected Transformations transformations;

    @Override
    public void initialize(final ExecutorService recordService, final Transformations transformations) {
        this.recordService = recordService;
        this.transformations = transformations;
    }

    @Override
    public abstract void processRecords(List<SourceRecord> records) throws Exception;

    public interface BatchProcessor<R> {
        void process(List<R> records) throws InterruptedException;

        class ObservableProcessor<R> implements BatchProcessor<R> {
            private final Logger LOGGER = LoggerFactory.getLogger(ObservableProcessor.class);
            private final DebeziumEngine.Watcher watcher;
            private final DebeziumEngine.RecordCommitter<R> committer;
            private final DebeziumEngine.ChangeConsumer<R> userHandler;

            public ObservableProcessor(DebeziumEngine.Watcher watcher,
                                       DebeziumEngine.RecordCommitter<R> committer,
                                       DebeziumEngine.ChangeConsumer<R> userHandler) {
                this.watcher = watcher;
                this.committer = committer;
                this.userHandler = userHandler;
            }

            @Override
            public void process(List<R> records) throws InterruptedException {
                if (watcher.engine().isConsuming()) {
                    LOGGER.trace("Calling user handler.");
                    userHandler.handleBatch(records, committer);
                }
            }
        }

        class DirectProcessor<R> implements BatchProcessor<R> {
            private final Logger LOGGER = LoggerFactory.getLogger(DirectProcessor.class);
            private final DebeziumEngine.RecordCommitter<R> committer;
            private final DebeziumEngine.ChangeConsumer<R> userHandler;

            public DirectProcessor(DebeziumEngine.RecordCommitter<R> committer,
                                   DebeziumEngine.ChangeConsumer<R> userHandler) {
                this.committer = committer;
                this.userHandler = userHandler;
            }

            @Override
            public void process(List<R> records) throws InterruptedException {
                LOGGER.trace("Calling user handler.");
                userHandler.handleBatch(records, committer);
            }
        }

    }

    public interface SingleProcessor<R> {
        void process(R record) throws InterruptedException;

        class ObservableSingleProcessor<R> implements SingleProcessor<R> {
            private final DebeziumEngine.Watcher watcher;
            private final Consumer<R> consumer;

            public ObservableSingleProcessor(DebeziumEngine.Watcher watcher, Consumer<R> consumer) {
                this.watcher = watcher;
                this.consumer = consumer;
            }

            @Override
            public void process(R record) throws InterruptedException {
                if (!watcher.engine().isConsuming()) {
                    return;
                }
                consumer.accept(record);
            }
        }

        class DirectSingleProcessor<R> implements SingleProcessor<R> {
            private final Consumer<R> consumer;

            public DirectSingleProcessor(Consumer<R> consumer) {
                this.consumer = consumer;
            }

            @Override
            public void process(R record) throws InterruptedException {
                consumer.accept(record);
            }
        }

    }

}
