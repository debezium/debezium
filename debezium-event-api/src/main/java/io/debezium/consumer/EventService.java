/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A service that provides access to one or more streams of change events.
 * 
 * @author Randall Hauch
 */
public interface EventService {

    /**
     * Obtain the names of the available streams.
     * 
     * @return the stream names; never null but possibly empty if no streams are available
     */
    Collection<String> listStreams();

    /**
     * Subscribe to one or more streams.
     * 
     * @param streamNames the names of the streams to which the resulting {@link EventStream} should subscribe
     * @return the component through which the events in the stream(s) can be obtained
     * @see #subscribe(Set)
     * @see #subscribe(Predicate)
     * @see #subscribeToAll()
     */
    default EventStream subscribe(String... streamNames) {
        return subscribe(new HashSet<>(Arrays.asList(streamNames)));
    }

    /**
     * Subscribe to one or more streams whose names match those in the given collection.
     * 
     * @param streamNames the names of the streams to which the resulting {@link EventStream} should subscribe
     * @return the component through which the events in the stream(s) can be obtained
     * @see #subscribe(String...)
     * @see #subscribe(Predicate)
     * @see #subscribeToAll()
     */
    default EventStream subscribe(Set<String> streamNames) {
        return subscribe(streamNames::contains);
    }

    /**
     * Subscribe to one or more streams whose names match the given predicate. As streams are added or removed, the
     * predicate is used to determine which streams are included in the subscription.
     * 
     * @param includeStreamNames the predicate function that will be called to determine if the names of existing or newly-added
     *            stream should be included in this subscription; may not be null
     * @return the component through which the events in the stream(s) can be obtained
     * @see #subscribe(String...)
     * @see #subscribeToAll()
     */
    EventStream subscribe(Predicate<String> includeStreamNames);

    /**
     * Subscribe to all available event streams, even those that are added after this method is called.
     * This is a convenience method that is the same as calling {@code subscribe((streamName)->true)}.
     * 
     * @return the component through which the events in the stream(s) can be obtained
     * @see #subscribe(Set)
     * @see #subscribe(String...)
     * @see #subscribe(Predicate)
     */
    default EventStream subscribeToAll() {
        return subscribe(streamName -> true);
    }
}
