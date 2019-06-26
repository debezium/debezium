/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

/**
 * An Event is a single unit that can be enqueued and processed by the {@link QueueProcessor}
 */
public interface Event {
    enum EventType {
        /**
         * A create, update, or delete event.
         */
        CHANGE_EVENT,

        /**
         * A hard delete followed by a delete event with the same key and null value,
         * this indicates Kafka log compaction, which removes all messages with the same key.
         */
        TOMBSTONE_EVENT,

        /**
         * An indicator representing a commit log segment has been processed,
         * or an error has occurred while processing.
         */
        EOF_EVENT
    }

    EventType getEventType();
}
