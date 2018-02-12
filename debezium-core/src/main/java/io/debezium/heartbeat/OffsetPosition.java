/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.util.Map;

public interface OffsetPosition {

    /**
     * Get the Kafka Connect detail about the source "partition", which describes the portion of the source that we are
     * consuming.
     * <p>
     * The resulting map is mutable for efficiency reasons (this information rarely changes), but should not be mutated.
     *
     * @return the source partition information; never null
     */
    Map<String, String> partition();

    /**
     * Get the Kafka Connect detail about the source "offset", which describes the position within the source where we last
     * have last read.
     *
     * @return a copy of the current offset; never null
     */
    Map<String, ?> offset();

    static OffsetPosition build(Map<String, String> partition, Map<String, ?> offset) {
        return new OffsetPosition() {

            @Override
            public Map<String, String> partition() {
                return partition;
            }

            @Override
            public Map<String, ?> offset() {
                return offset;
            }
        };
    }
}
