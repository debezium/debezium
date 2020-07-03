package io.debezium.embedded.reactive;

import io.debezium.engine.OffsetCommitter;
import io.debezium.engine.reactive.CommittableRecord;

public class DefaultCommittableRecord<R> implements CommittableRecord<R> {
    private final OffsetCommitter<R> committer;
    private final R record;

    public DefaultCommittableRecord(OffsetCommitter<R> committer, R record) {
        this.committer = committer;
        this.record = record;
    }

    @Override
    public OffsetCommitter<R> committer() {
        return committer;
    }

    @Override
    public R record() {
        return record;
    }
}
