/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;

import io.debezium.annotation.NotThreadSafe;

/**
 * Listeners for cache evictions when a cache reaches full capacity.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class EhcacheEvictionListener implements CacheEventListener<Object, Object> {

    private boolean evictionSeen = false;

    @Override
    public void onEvent(CacheEvent<?, ?> event) {
        // Ehcache will not propagate the exception if thrown inside a CacheEventListener.
        // Therefore, each cache will check the state of the listener after each put.
        // Given that we don't perform concurrent thread access to caches, it's safe to
        // use a non-atomic here for performance.
        if (EventType.EVICTED == event.getType()) {
            this.evictionSeen = true;
        }
    }

    /**
     * Check whether an eviction event has been seen by the cache.
     *
     * @return {@code true} if an eviction occurred, {@code false} otherwise
     */
    public boolean hasEvictionBeenSeen() {
        return evictionSeen;
    }

}
