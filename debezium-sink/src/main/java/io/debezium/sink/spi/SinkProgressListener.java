/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.spi;

/**
 * Invoked whenever an important event or change of state happens during sink record processing.
 */
public interface SinkProgressListener {

    void written(long count);

    void deleted(long count);

    void truncated();

    void filtered();

    /**
     * Invoked when records are routed to the errant record reporter (dead letter queue)
     * instead of being written to the target system.
     */
    void errantRecordsReported(long count);

    void tableCreated();

    void tableAltered();

    static SinkProgressListener NO_OP() {
        return new SinkProgressListener() {

            @Override
            public void written(long count) {
            }

            @Override
            public void deleted(long count) {
            }

            @Override
            public void truncated() {
            }

            @Override
            public void filtered() {
            }

            @Override
            public void errantRecordsReported(long count) {
            }

            @Override
            public void tableCreated() {
            }

            @Override
            public void tableAltered() {
            }
        };
    }
}
