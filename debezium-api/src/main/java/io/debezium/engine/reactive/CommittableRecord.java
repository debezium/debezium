package io.debezium.engine.reactive;

import io.debezium.engine.OffsetCommitter;

public interface CommittableRecord<R> {
    OffsetCommitter<R> committer();

    R record();

    default void markAsProcessed() throws InterruptedException {
        committer().markAsProcessed(record());
    }
}