package io.debezium.embedded.reactive;

import io.debezium.engine.OffsetCommitter;
import io.debezium.engine.RecordCommitter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultOffsetCommitter<R> implements OffsetCommitter<R> {
    private final List<R> records;
    private final RecordCommitter<R> committer;
    private final Set<R> processedRecords;

    public DefaultOffsetCommitter(List<R> records, RecordCommitter<R> committer) {
        this.records = records;
        this.committer = committer;
        this.processedRecords = new HashSet<>();
    }

    @Override
    public List<R> records() {
        return records;
    }

    @Override
    public RecordCommitter<R> committer() {
        return committer;
    }

    @Override
    public synchronized void markAsProcessed(R r) throws InterruptedException {
        committer.markProcessed(r);
        processedRecords.add(r);

        if (processedRecords.size() == records.size()) {
            committer.markBatchFinished();
        }
    }
}
