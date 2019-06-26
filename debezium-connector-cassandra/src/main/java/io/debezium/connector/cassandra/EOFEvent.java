/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;

/**
 * An EOFEvent is an event that indicates a commit log has been processed (successfully or not).
 */
public class EOFEvent implements Event {
    public final File file;
    public final boolean success;

    public EOFEvent(File file, boolean success) {
        this.file = file;
        this.success = success;
    }

    @Override
    public EventType getEventType() {
        return EventType.EOF_EVENT;
    }
}
