/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import com.mongodb.client.ChangeStreamIterable;

/**
 * Interface allowing implementers to update the change stream in response to an event
 * @param <TResult>
 */
public interface StreamManager<TResult> {
    ChangeStreamIterable<TResult> initStream(ChangeStreamIterable<TResult> stream);

    ChangeStreamIterable<TResult> updateStream(ChangeStreamIterable<TResult> stream);

    boolean shouldUpdateStream(BufferingChangeStreamCursor.ResumableChangeStreamEvent<TResult> event);
}
