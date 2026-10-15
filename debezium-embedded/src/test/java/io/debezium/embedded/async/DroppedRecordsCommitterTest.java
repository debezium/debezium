/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.engine.DebeziumEngine;

/**
 * Verifies that {@link DroppedRecordsCommitter} marks the records dropped by a transformation in source order, without getting ahead
 * of the records that the handler marks.
 */
public class DroppedRecordsCommitterTest {

    @Test
    void marksDroppedRecordsInSourceOrder() throws Exception {
        final List<SourceRecord> source = records(5);
        final List<SourceRecord> results = Arrays.asList(source.get(0), null, source.get(2), null, null);
        final RecordingCommitter recorder = new RecordingCommitter();
        final DroppedRecordsCommitter<SourceRecord> committer = new DroppedRecordsCommitter<>(recorder, recorder, source, results);

        committer.markProcessed(source.get(0));
        assertThat(recorder.calls).containsExactly("0");

        committer.markProcessed(source.get(2));
        assertThat(recorder.calls).containsExactly("0", "1", "2");

        committer.markBatchFinished();
        assertThat(recorder.calls).containsExactly("0", "1", "2", "3", "4", "finished");
    }

    @Test
    void marksAllRecordsWhenEveryRecordIsDropped() throws Exception {
        final List<SourceRecord> source = records(3);
        final RecordingCommitter recorder = new RecordingCommitter();
        final DroppedRecordsCommitter<SourceRecord> committer = new DroppedRecordsCommitter<>(recorder, recorder, source,
                Arrays.asList(null, null, null));

        committer.markBatchFinished();
        assertThat(recorder.calls).containsExactly("0", "1", "2", "finished");
    }

    @Test
    void doesNotMarkDroppedRecordsAfterTheLastMarkedRecordUntilTheBatchFinishes() throws Exception {
        final List<SourceRecord> source = records(4);
        final List<SourceRecord> results = Arrays.asList(null, source.get(1), null, source.get(3));
        final RecordingCommitter recorder = new RecordingCommitter();
        final DroppedRecordsCommitter<SourceRecord> committer = new DroppedRecordsCommitter<>(recorder, recorder, source, results);

        // The handler fails after the first record it receives, so the record dropped after it must not be marked
        committer.markProcessed(source.get(1));
        assertThat(recorder.calls).containsExactly("0", "1");
    }

    @Test
    void doesNotMarkDroppedRecordsAfterARecordNotMarkedWhenTheBatchFinishes() throws Exception {
        final List<SourceRecord> source = records(5);
        final List<SourceRecord> results = Arrays.asList(source.get(0), source.get(1), null, source.get(3), null);
        final RecordingCommitter recorder = new RecordingCommitter();
        final DroppedRecordsCommitter<SourceRecord> committer = new DroppedRecordsCommitter<>(recorder, recorder, source, results);

        // The handler is stopped after the second record it receives and finishes the batch without marking the third one
        committer.markProcessed(source.get(0));
        committer.markProcessed(source.get(1));
        committer.markBatchFinished();
        assertThat(recorder.calls).containsExactly("0", "1", "2", "finished");
    }

    @Test
    void marksDroppedRecordsBeforeRecordMarkedWithOffsets() throws Exception {
        final List<SourceRecord> source = records(3);
        final List<SourceRecord> results = Arrays.asList(null, null, source.get(2));
        final RecordingCommitter recorder = new RecordingCommitter();
        final DroppedRecordsCommitter<SourceRecord> committer = new DroppedRecordsCommitter<>(recorder, recorder, source, results);

        committer.markProcessed(source.get(2), committer.buildOffsets());
        committer.markBatchFinished();
        assertThat(recorder.calls).containsExactly("0", "1", "2+offsets", "finished");
    }

    private static List<SourceRecord> records(int count) {
        final List<SourceRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(new SourceRecord(Map.of("partition", 0), Map.of("position", i), "topic", null, String.valueOf(i)));
        }
        return records;
    }

    /**
     * Records the calls it receives, using the value of each record as its name.
     */
    private static class RecordingCommitter implements DebeziumEngine.RecordCommitter<SourceRecord> {

        final List<String> calls = new ArrayList<>();

        @Override
        public void markProcessed(SourceRecord record) {
            calls.add(String.valueOf(record.value()));
        }

        @Override
        public void markBatchFinished() {
            calls.add("finished");
        }

        @Override
        public void markProcessed(SourceRecord record, DebeziumEngine.Offsets sourceOffsets) {
            calls.add(record.value() + "+offsets");
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return (key, value) -> {
            };
        }
    }
}
