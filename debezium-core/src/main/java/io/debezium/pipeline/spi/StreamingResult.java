/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class StreamingResult<O extends OffsetContext> {

    private final StreamingResultStatus status;
    private final O offset;

    private StreamingResult(StreamingResultStatus status, O offset) {
        this.status = status;
        this.offset = offset;
    }

    public static <O extends OffsetContext> StreamingResult<O> streamingNotEnabled(O offset) {
        return new StreamingResult<>(StreamingResultStatus.STREAMING_NOT_ENABLED, offset);
    }

    public static <O extends OffsetContext> StreamingResult<O> noChangesInDatabase(O offset) {
        return new StreamingResult<>(StreamingResultStatus.NO_CHANGES_IN_DATABASE, offset);
    }

    public static <O extends OffsetContext> StreamingResult<O> noMaximumLsnRecorded(O offset) {
        return new StreamingResult<>(StreamingResultStatus.NO_MAXIMUM_LSN_RECORDED, offset);
    }

    public static <O extends OffsetContext> StreamingResult<O> changesDetected(O offset) {
        return new StreamingResult<>(StreamingResultStatus.CHANGES_IN_DATABASE, offset);
    }

    public boolean hasNoEvents() {
        return this.status == StreamingResultStatus.NO_CHANGES_IN_DATABASE || this.status == StreamingResultStatus.NO_MAXIMUM_LSN_RECORDED;
    }

    public StreamingResultStatus getStatus() {
        return status;
    }

    public O getOffset() {
        return offset;
    }

    public static enum StreamingResultStatus {
        STREAMING_NOT_ENABLED,
        NO_CHANGES_IN_DATABASE,
        NO_MAXIMUM_LSN_RECORDED,
        CHANGES_IN_DATABASE
    }

    @Override
    public String toString() {
        return "SnapshotResult [status=" + status + ", offset=" + offset + "]";
    }
}
