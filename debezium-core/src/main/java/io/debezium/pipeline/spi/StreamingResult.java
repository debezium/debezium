/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class StreamingResult<O extends OffsetContext> {

    private final O offset;

    public StreamingResult(O offset) {
        this.offset = offset;
    }

    public O getOffset() {
        return offset;
    }

    public boolean eventsStreamed() {
        return offset.eventsStreamed();
    }

    @Override
    public String toString() {
        return "StreamingResult [offset=" + offset + "]";
    }
}
