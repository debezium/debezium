/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;

/**
 * An in-memory, per-process {@link ReselectColumnCache} backed by a bounded, LRU-evicting map of rows,
 * each holding its own per-column map. Organising the cache as {@code row -> column} means the row
 * identity is resolved once per {@link #forRow(Struct)} call and reused for all of that row's columns.
 * <p>
 * The row identity is the event's message key {@link Struct}, used directly as the outer map key. The
 * {@code Struct} {@code equals}/{@code hashCode} contract compares the key schema together with the
 * (deep, content-based) field values, so binary key values are compared by content and any change to the
 * key's schema (DDL, primary-key reordering, default-value changes, etc.) yields a different key and a
 * natural cache miss rather than a false hit.
 * <p>
 * Entries are bounded by an LRU limit on the number of cached rows and by a per-value time-to-live;
 * because the post-processor refreshes entries on modification, the TTL only bounds memory retention for
 * rows that are no longer being updated and is not the mechanism that guarantees freshness.
 *
 * @author Gaurav Miglani
 */
public class MemoryReselectColumnCache implements ReselectColumnCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryReselectColumnCache.class);

    public static final String MAX_SIZE = "reselect.cache.max.size";
    public static final String TTL_MS = "reselect.cache.ttl.ms";

    private static final int DEFAULT_MAX_SIZE = 10_000;
    private static final long DEFAULT_TTL_MS = 600_000L;

    private long ttlMs;
    // message key struct -> (column -> value). The outer map is bounded/LRU on the number of rows.
    private BoundedConcurrentHashMap<Struct, Map<String, CachedValue>> rows;

    @Override
    public void configure(Configuration config) {
        final int maxSize = config.getInteger(MAX_SIZE, DEFAULT_MAX_SIZE);
        this.ttlMs = config.getLong(TTL_MS, DEFAULT_TTL_MS);
        this.rows = new BoundedConcurrentHashMap<>(maxSize, 16, Eviction.LRU);
        LOGGER.info("Initialized in-memory reselect cache with max {} rows and TTL {} ms.", maxSize, ttlMs);
    }

    @Override
    public RowCache forRow(Struct messageKey) {
        return new MemoryRowCache(messageKey);
    }

    @Override
    public void close() {
        if (rows != null) {
            rows.clear();
        }
    }

    private final class MemoryRowCache implements RowCache {

        private final Struct rowKey;

        private MemoryRowCache(Struct rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public Optional<Hit> get(String column) {
            final Map<String, CachedValue> columns = rows.get(rowKey);
            if (columns == null) {
                return Optional.empty();
            }
            final CachedValue cached = columns.get(column);
            if (cached == null || cached.isExpired(System.currentTimeMillis(), ttlMs)) {
                return Optional.empty();
            }
            // A present entry is a hit even when its value is null; the Hit holder preserves that.
            return Optional.of(new Hit(cached.value));
        }

        @Override
        public void put(String column, Object value) {
            rows.computeIfAbsent(rowKey, k -> new ConcurrentHashMap<>())
                    .put(column, new CachedValue(value, System.currentTimeMillis()));
        }

        @Override
        public void invalidate(String column) {
            final Map<String, CachedValue> columns = rows.get(rowKey);
            if (columns != null) {
                // Leave an emptied row map in place; the outer LRU bounds the number of rows. Removing it
                // here would race with a concurrent put() for the same row.
                columns.remove(column);
            }
        }
    }

    /**
     * A single cached column value tagged with the wall-clock time it was stored, used to enforce TTL.
     * The value itself may be {@code null}; presence of the {@code CachedValue} (not the value) denotes a
     * cache hit.
     */
    private static final class CachedValue {
        private final Object value;
        private final long timestampMs;

        private CachedValue(Object value, long timestampMs) {
            this.value = value;
            this.timestampMs = timestampMs;
        }

        private boolean isExpired(long nowMs, long ttlMs) {
            return nowMs - timestampMs >= ttlMs;
        }
    }
}