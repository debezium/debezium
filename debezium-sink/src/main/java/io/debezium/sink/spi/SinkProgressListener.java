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
            public void tableCreated() {
            }

            @Override
            public void tableAltered() {
            }
        };
    }
}
