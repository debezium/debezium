package io.debezium.engine;

import java.util.List;

public interface OffsetCommitter<R> {
    List<R> records();
    RecordCommitter<R> committer();
    void markAsProcessed(R r) throws InterruptedException;
}
