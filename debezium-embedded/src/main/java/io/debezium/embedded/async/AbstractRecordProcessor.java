/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.Transformations;
import io.debezium.engine.DebeziumEngine;

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

    /**
     * Passes the results of processing a batch to the batch processor, without the records dropped by a transformation.
     * The offsets of the dropped records must still be committed, so in that case the batch processor gets a committer that also marks them.
     *
     * @param processor the batch processor
     * @param records the records of the batch, in source order
     * @param results the result of processing each record, at the same position; {@code null} for a dropped record
     * @param committer the committer of the processed records
     * @param sourceCommitter the committer used to mark the dropped records, sharing the offset state with {@code committer}
     */
    protected static <T> void processBatch(BatchProcessor<T> processor, List<SourceRecord> records, List<T> results,
                                           DebeziumEngine.RecordCommitter<T> committer,
                                           DebeziumEngine.RecordCommitter<SourceRecord> sourceCommitter)
            throws InterruptedException {
        if (!results.contains(null)) {
            processor.process(results, committer);
            return;
        }
        final List<T> processedRecords = new ArrayList<>(results.size());
        for (T result : results) {
            if (result != null) {
                processedRecords.add(result);
            }
        }
        processor.process(processedRecords, new DroppedRecordsCommitter<>(committer, sourceCommitter, records, results));
    }

    public interface BatchProcessor<R> {
        void process(List<R> records, DebeziumEngine.RecordCommitter<R> committer) throws InterruptedException;

        class ObservableProcessor<R> implements BatchProcessor<R> {
            private final Logger LOGGER = LoggerFactory.getLogger(ObservableProcessor.class);
            private final Watcher watcher;
            private final DebeziumEngine.ChangeConsumer<R> userHandler;

            public ObservableProcessor(Watcher watcher,
                                       DebeziumEngine.ChangeConsumer<R> userHandler) {
                this.watcher = watcher;
                this.userHandler = userHandler;
            }

            @Override
            public void process(List<R> records, DebeziumEngine.RecordCommitter<R> committer) throws InterruptedException {
                if (watcher.engine().isPolling()) {
                    LOGGER.trace("Calling user handler.");
                    userHandler.handleBatch(records, committer);
                }
            }
        }

        class DirectProcessor<R> implements BatchProcessor<R> {
            private final Logger LOGGER = LoggerFactory.getLogger(DirectProcessor.class);
            private final DebeziumEngine.ChangeConsumer<R> userHandler;

            public DirectProcessor(DebeziumEngine.ChangeConsumer<R> userHandler) {
                this.userHandler = userHandler;
            }

            @Override
            public void process(List<R> records, DebeziumEngine.RecordCommitter<R> committer) throws InterruptedException {
                LOGGER.trace("Calling user handler.");
                userHandler.handleBatch(records, committer);
            }
        }

    }

    public interface SingleProcessor<R> {
        void process(R record) throws InterruptedException;

        class ObservableSingleProcessor<R> implements SingleProcessor<R> {
            private final Watcher watcher;
            private final Consumer<R> consumer;

            public ObservableSingleProcessor(Watcher watcher, Consumer<R> consumer) {
                this.watcher = watcher;
                this.consumer = consumer;
            }

            @Override
            public void process(R record) throws InterruptedException {
                if (!watcher.engine().isPolling()) {
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
