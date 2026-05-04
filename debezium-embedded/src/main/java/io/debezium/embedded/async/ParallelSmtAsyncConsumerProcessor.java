/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.Transformations;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Watcher;
import io.debezium.engine.StopEngineException;

/**
 * {@link RecordProcessor} which transforms the records in parallel. Records are passed to the user-provided {@link Consumer} in arbitrary order, once they are
 * processed. This processor should be used when user provides only custom {@link Consumer} and records should be passed without converting to the consumer in the same
 * order as they were obtained from the database.
 *
 * @author vjuranek
 */
public class ParallelSmtAsyncConsumerProcessor extends AbstractRecordProcessor<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAsyncConsumerProcessor.class);

    private final DebeziumEngine.RecordCommitter committer;
    private final Function<SourceRecord, ProcessingCallables.TransformAndConsumeRecord> transformation;

    ParallelSmtAsyncConsumerProcessor(final DebeziumEngine.RecordCommitter committer,
                                      Function<SourceRecord, ProcessingCallables.TransformAndConsumeRecord> transformation) {
        this.committer = committer;
        this.transformation = transformation;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final Future<Void>[] recordFutures = new Future[records.size()];
        Iterator<SourceRecord> recordsIterator = records.iterator();
        for (int i = 0; recordsIterator.hasNext(); i++) {
            recordFutures[i] = recordService.submit(transformation.apply(recordsIterator.next()));
        }

        LOGGER.trace("Waiting for the batch to finish processing.");
        recordsIterator = records.iterator();
        for (int i = 0; recordsIterator.hasNext(); i++) {
            try {
                recordFutures[i].get();
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof StopEngineException) {
                    committer.markProcessed(recordsIterator.next());
                }
                throw e;
            }
            committer.markProcessed(recordsIterator.next());
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }

    public static <R> ParallelSmtAsyncConsumerProcessor create(DebeziumEngine.RecordCommitter<SourceRecord> committer,
                                                               Consumer<SourceRecord> consumer,
                                                               DebeziumShutdown<R> shutdown, Runnable workflow,
                                                               Transformations transformations,
                                                               Watcher watcher) {
        if (shutdown == null) {
            return new ParallelSmtAsyncConsumerProcessor(committer,
                    record -> new ProcessingCallables.TransformAndConsumeRecord(record,
                            transformations,
                            consumer));
        }

        return new ParallelSmtAsyncConsumerProcessor(committer,
                record -> new ProcessingCallables.TransformAndConsumeRecord(record,
                        transformations,
                        new ShutdownConsumer<>(
                                (ShutdownHandler<SourceRecord>) DefaultShutdownHandler.create(shutdown.before(), workflow, committer),
                                (ShutdownHandler<SourceRecord>) DefaultShutdownHandler.create(shutdown.after(), workflow, committer),
                                transformedRecord -> {
                                    if (transformedRecord != null && watcher.engine().isConsuming()) {
                                        consumer.accept(transformedRecord);
                                    }
                                })));
    }
}
