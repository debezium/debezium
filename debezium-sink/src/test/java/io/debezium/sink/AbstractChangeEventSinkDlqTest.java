/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.dlq.ErrorReporter;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.BatchRecord;

class AbstractChangeEventSinkDlqTest {

    private final List<DebeziumSinkRecord> reported = new ArrayList<>();
    private final ErrorReporter recordingReporter = (record, e) -> reported.add(record);

    @FixFor("debezium/dbz#984")
    @Test
    void writeBatchWithoutReporterShouldPropagateFailure() {
        FailingChangeEventSink sink = new FailingChangeEventSink(null, record -> true, false);

        assertThatThrownBy(() -> sink.writeBatch(batchOf("a", "b")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("write failed");
        assertThat(reported).isEmpty();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void writeBatchShouldIsolateAndReportOnlyFailingRecords() {
        FailingChangeEventSink sink = new FailingChangeEventSink(recordingReporter, named("b"), false);

        sink.writeBatch(batchOf("a", "b", "c"));

        assertThat(reported).hasSize(1);
        assertThat(name(reported.get(0))).isEqualTo("b");
        assertThat(sink.writtenRecordNames).containsExactly("a", "c");
        assertThat(sink.getTotalRecordsWritten()).isEqualTo(2);
        assertThat(sink.getTotalRecordsReported()).isEqualTo(1);
    }

    @FixFor("debezium/dbz#984")
    @Test
    void writeBatchShouldReportSingleRecordBatchWithoutReplay() {
        FailingChangeEventSink sink = new FailingChangeEventSink(recordingReporter, named("a"), false);

        sink.writeBatch(batchOf("a"));

        assertThat(reported).hasSize(1);
        assertThat(sink.writeAttempts).isEqualTo(1);
        assertThat(sink.getTotalRecordsWritten()).isZero();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void writeBatchShouldPropagateRetriableFailuresInsteadOfReporting() {
        FailingChangeEventSink sink = new FailingChangeEventSink(recordingReporter, record -> true, true);

        assertThatThrownBy(() -> sink.writeBatch(batchOf("a", "b")))
                .isInstanceOf(RuntimeException.class);
        assertThat(reported).isEmpty();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void unrollShouldPropagateRetriableFailureMidway() {
        FailingChangeEventSink sink = new FailingChangeEventSink(recordingReporter, named("b"), false);
        sink.retriableAfterFirstFailure = true;

        assertThatThrownBy(() -> sink.writeBatch(batchOf("a", "b", "c")))
                .isInstanceOf(RuntimeException.class);
        assertThat(reported).isEmpty();
        assertThat(sink.writtenRecordNames).containsExactly("a");
    }

    @FixFor("debezium/dbz#984")
    @Test
    void throwingReporterShouldPropagateAndStopUnroll() {
        // When errors.tolerance=none the Kafka Connect errant record reporter throws on report();
        // the failure must propagate so the task fails as it would without a reporter.
        ErrorReporter throwingReporter = (record, e) -> {
            throw new RuntimeException("tolerance is none");
        };
        FailingChangeEventSink sink = new FailingChangeEventSink(throwingReporter, named("b"), false);

        assertThatThrownBy(() -> sink.writeBatch(batchOf("a", "b", "c")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("tolerance is none");
        assertThat(sink.writtenRecordNames).containsExactly("a");
    }

    @FixFor("debezium/dbz#984")
    @Test
    void writeBatchWithReporterShouldNotInterceptSuccessfulBatches() {
        FailingChangeEventSink sink = new FailingChangeEventSink(recordingReporter, record -> false, false);

        sink.writeBatch(batchOf("a", "b"));

        assertThat(reported).isEmpty();
        assertThat(sink.writtenRecordNames).containsExactly("a", "b");
        assertThat(sink.writeAttempts).isEqualTo(1);
    }

    private static Predicate<DebeziumSinkRecord> named(String name) {
        return record -> name.equals(name(record));
    }

    private static String name(DebeziumSinkRecord record) {
        return record.getPayload().getString("name");
    }

    private static Batch batchOf(String... names) {
        List<BatchRecord> records = new ArrayList<>();
        byte id = 1;
        for (String name : names) {
            KafkaDebeziumSinkRecord record = TestSinkRecords.flat("topic", id++, name);
            records.add(new BatchRecord(new CollectionId("topic"), record));
        }
        return new Batch(records);
    }

    private static class FailingChangeEventSink extends AbstractChangeEventSink {

        final List<String> writtenRecordNames = new ArrayList<>();
        int writeAttempts;
        boolean retriableAfterFirstFailure;

        private final Predicate<DebeziumSinkRecord> failOn;
        private final boolean retriable;
        private boolean failed;

        FailingChangeEventSink(ErrorReporter errorReporter, Predicate<DebeziumSinkRecord> failOn, boolean retriable) {
            super(new TestSinkConnectorConfig(), errorReporter);
            this.failOn = failOn;
            this.retriable = retriable;
        }

        @Override
        protected void doWriteBatch(Batch batch) {
            writeAttempts++;
            for (BatchRecord batchRecord : batch) {
                if (failOn.test(batchRecord.record())) {
                    boolean asRetriable = retriable || (retriableAfterFirstFailure && failed);
                    failed = true;
                    throw asRetriable ? new TestRetriableException() : new RuntimeException("write failed");
                }
            }
            batch.forEach(batchRecord -> writtenRecordNames.add(name(batchRecord.record())));
        }

        @Override
        protected boolean isRetriableWriteException(RuntimeException exception) {
            return exception instanceof TestRetriableException;
        }

        @Override
        public CollectionId getCollectionId(String collectionName) {
            return new CollectionId(collectionName);
        }

        @Override
        public void close() {
        }
    }

    private static class TestRetriableException extends RuntimeException {
    }
}
