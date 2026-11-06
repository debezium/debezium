/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.engine.DebeziumEngine;

/**
 * The final offset commit happens while the engine stops. If that commit fails, the engine must not report a successful completion,
 * otherwise the offsets of the delivered records are lost without any signal.
 */
class FinalOffsetCommitFailureTest extends AbstractAsyncEngineConnectorTest {

    private static final int RECORDS_PER_BATCH = 5;

    @BeforeEach
    void resetOffsetStore() {
        FailingOffsetStore.failWrites.set(false);
        FailingOffsetStore.writtenValues.clear();
    }

    @Test
    void shouldNotCompleteSuccessfullyWhenFinalOffsetCommitFails() throws Exception {
        // The writes fail before the engine starts, so neither the per-batch commit nor the final commit can store the offsets.
        FailingOffsetStore.failWrites.set(true);
        final CountDownLatch recordsConsumed = new CountDownLatch(RECORDS_PER_BATCH);
        final AtomicReference<Boolean> completionSuccess = new AtomicReference<>();
        final AtomicReference<Throwable> completionError = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);

        final DebeziumEngine.ChangeConsumer<SourceRecord> consumer = (records, committer) -> {
            for (SourceRecord record : records) {
                committer.markProcessed(record);
            }
            committer.markBatchFinished();
            // Counted down only after the batch is finished, so close() can't interrupt the per-batch commit.
            records.forEach(record -> recordsConsumed.countDown());
        };

        start(SimpleSourceConnector.class, connectorConfig(), (success, message, error) -> {
            completionSuccess.set(success);
            completionError.set(error);
            completed.countDown();
        }, null, x -> {
        }, true, consumer, null);

        assertThat(recordsConsumed.await(30, TimeUnit.SECONDS)).isTrue();
        engine.close();

        assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(completionSuccess.get()).as("completion must not report success when the offsets were not stored").isFalse();
        assertThat(completionError.get())
                .as("completion must report the failed final commit")
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("the offsets were not committed")
                .hasRootCauseMessage("Injected offset write failure");
    }

    @Test
    void shouldReportFinalOffsetCommitFailureAlongWithTheEngineFailure() throws Exception {
        // The consumer fails after marking the records, so the engine fails and the final commit, which can't store the
        // offsets either, must still be visible to the completion callback.
        FailingOffsetStore.failWrites.set(true);
        final AtomicReference<Boolean> completionSuccess = new AtomicReference<>();
        final AtomicReference<Throwable> completionError = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);

        final DebeziumEngine.ChangeConsumer<SourceRecord> consumer = (records, committer) -> {
            for (SourceRecord record : records) {
                committer.markProcessed(record);
            }
            throw new IllegalStateException("Injected consumer failure");
        };

        start(SimpleSourceConnector.class, connectorConfig(), (success, message, error) -> {
            completionSuccess.set(success);
            completionError.set(error);
            completed.countDown();
        }, null, x -> {
        }, true, consumer, null);

        assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(completionSuccess.get()).isFalse();
        assertThat(completionError.get())
                .as("completion must report the failure of the engine")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Injected consumer failure");
        assertThat(completionError.get().getSuppressed())
                .as("the failed final commit must be kept as a suppressed exception of the reported error")
                .anySatisfy(suppressed -> assertThat(suppressed)
                        .isInstanceOf(DebeziumException.class)
                        .hasRootCauseMessage("Injected offset write failure"));
    }

    @Test
    void shouldCompleteSuccessfullyAndStoreLastOffsetWhenFinalOffsetCommitSucceeds() throws Exception {
        // The consumer never calls markBatchFinished, so the only commit of the offsets is the final one done while the engine stops.
        final CountDownLatch recordsConsumed = new CountDownLatch(RECORDS_PER_BATCH);
        final AtomicReference<Boolean> completionSuccess = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);

        final DebeziumEngine.ChangeConsumer<SourceRecord> consumer = (records, committer) -> {
            for (SourceRecord record : records) {
                committer.markProcessed(record);
                recordsConsumed.countDown();
            }
        };

        start(SimpleSourceConnector.class, connectorConfig(), (success, message, error) -> {
            completionSuccess.set(success);
            completed.countDown();
        }, null, x -> {
        }, true, consumer, null);

        assertThat(recordsConsumed.await(30, TimeUnit.SECONDS)).isTrue();
        engine.close();

        assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(completionSuccess.get()).isTrue();
        // The offset of the last record is the one left by the final commit, it must have been stored.
        assertThat(FailingOffsetStore.writtenValues)
                .as("the final commit must store the offset of the last record")
                .anyMatch(value -> value.contains("\"id\":" + RECORDS_PER_BATCH));
    }

    @Test
    void shouldCompleteSuccessfullyWhenTaskCommitFailsAfterTheOffsetsAreStored() throws Exception {
        // The offsets are stored before the task is told about the commit, so a failing task commit must not fail the completion.
        final CountDownLatch recordsConsumed = new CountDownLatch(RECORDS_PER_BATCH);
        final AtomicReference<Boolean> completionSuccess = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);

        final DebeziumEngine.ChangeConsumer<SourceRecord> consumer = (records, committer) -> {
            for (SourceRecord record : records) {
                committer.markProcessed(record);
                recordsConsumed.countDown();
            }
        };

        start(FailingCommitConnector.class, connectorConfig(), (success, message, error) -> {
            completionSuccess.set(success);
            completed.countDown();
        }, null, x -> {
        }, true, consumer, null);

        assertThat(recordsConsumed.await(30, TimeUnit.SECONDS)).isTrue();
        engine.close();

        assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(completionSuccess.get()).as("a failed task commit is not a failed offset flush").isTrue();
        assertThat(FailingOffsetStore.writtenValues)
                .as("the final commit must store the offset of the last record")
                .anyMatch(value -> value.contains("\"id\":" + RECORDS_PER_BATCH));
    }

    private static Configuration connectorConfig() {
        return Configuration.create()
                .with(SimpleSourceConnector.BATCH_COUNT, 1)
                .with(SimpleSourceConnector.RECORD_COUNT_PER_BATCH, RECORDS_PER_BATCH)
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, FailingOffsetStore.class.getName())
                .build();
    }

    /**
     * Offset store whose writes can be made to fail on demand. The values of the successful writes are kept for the assertions.
     */
    public static class FailingOffsetStore extends MemoryOffsetBackingStore {

        static final AtomicBoolean failWrites = new AtomicBoolean();
        static final List<String> writtenValues = new CopyOnWriteArrayList<>();

        @Override
        public Set<Map<String, Object>> connectorPartitions(String connectorName) {
            return Set.of();
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            if (failWrites.get()) {
                final IOException failure = new IOException("Injected offset write failure");
                if (callback != null) {
                    callback.onCompletion(failure, null);
                }
                return CompletableFuture.failedFuture(failure);
            }
            for (ByteBuffer value : values.values()) {
                if (value != null) {
                    writtenValues.add(StandardCharsets.UTF_8.decode(value.duplicate()).toString());
                }
            }
            return super.set(values, callback);
        }
    }

    /**
     * Connector whose task fails when it is told that the offsets were committed.
     */
    public static class FailingCommitConnector extends SimpleSourceConnector {

        @Override
        public Class<? extends Task> taskClass() {
            return FailingCommitTask.class;
        }
    }

    public static class FailingCommitTask extends SimpleSourceConnector.SimpleConnectorTask {

        @Override
        public void commit() {
            throw new IllegalStateException("Injected task commit failure");
        }
    }
}
