/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect.cache;

import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;

/**
 * A strategy contract for caching re-selected column values, organised hierarchically as
 * {@code row -> column}. The reselect post-processor consults the cache before issuing a database query
 * and stores the results afterwards, allowing repeated re-selections of the same column to be served
 * from the cache.
 * <p>
 * Access is row-scoped: a caller first resolves a {@link RowCache} for a given row via
 * {@link #forRow(Struct)} and then performs per-column operations on it. This lets implementations
 * resolve the (potentially expensive) row identity once per event and amortise it across all columns of
 * that row, rather than re-deriving it for every column.
 * <p>
 * The row identity is the event's message key {@link Struct}. Using the key {@code Struct} directly means
 * the cache identity is governed by the same factors that determine the key on the topic: a primary-key
 * reordering, a DDL change, a default-value change or any other change that alters the key's schema or
 * serialized values yields a different key and therefore a natural cache miss, never a false hit against a
 * stale entry. This is important for persistent backends (e.g. an embedded key/value store) where a
 * reliable, self-describing key is required.
 * <p>
 * Cached values are the <em>final converted</em> values as they appear in the event's {@code after}
 * struct, so a cache hit can be applied to the event directly without re-conversion. A cached value may be
 * {@code null} (e.g. a {@code null} LOB column); implementations must preserve and return such values
 * rather than treating them as a miss, which is why {@link RowCache#get(String)} returns a {@link Hit}
 * holder instead of the value directly.
 * <p>
 * Implementations decide how values are stored (e.g. in-memory, embedded key/value store) and how they
 * expire. Implementations are obtained via a no-argument constructor and then {@link #configure(Configuration)}
 * is invoked, following the standard Debezium configurable lifecycle.
 * <p>
 * Implementations must be thread-safe.
 *
 * @author Gaurav Miglani
 */
@Incubating
public interface ReselectColumnCache extends AutoCloseable {

    /**
     * Configure the cache. Invoked once after construction, before any other method.
     *
     * @param config the connector configuration, including any cache-specific properties
     */
    void configure(Configuration config);

    /**
     * Resolve a row-scoped view of the cache for the row identified by the given message key. The row
     * identity is resolved once here and reused for all subsequent per-column operations on the returned
     * handle.
     *
     * @param messageKey the event's message key struct identifying the row; may not be null
     * @return a row-scoped cache handle
     */
    RowCache forRow(Struct messageKey);

    /**
     * Release any resources held by the cache. Invoked when the post-processor is closed.
     */
    @Override
    void close();

    /**
     * A row-scoped view of the cache, bound to a single row. All operations act on columns of that one row.
     */
    interface RowCache {

        /**
         * Return the cached value for the given column, if present and still valid.
         * <p>
         * A returned {@link Hit} signals a cache hit and carries the cached value, which may itself be
         * {@code null}. {@link Optional#empty()} signals a cache miss. This distinction lets a genuinely
         * cached {@code null} value be reused rather than re-queried.
         *
         * @param column the column name
         * @return a {@link Hit} on a cache hit, or {@link Optional#empty()} on a miss
         */
        Optional<Hit> get(String column);

        /**
         * Store the value for the given column. The value is the final converted value as it appears in
         * the event's {@code after} struct and may be {@code null}.
         *
         * @param column the column name
         * @param value the value to cache; may be null
         */
        void put(String column, Object value);

        /**
         * Evict the cached value for the given column, if any.
         *
         * @param column the column name
         */
        void invalidate(String column);
    }

    /**
     * Holder returned by {@link RowCache#get(String)} on a cache hit. Distinguishes a cached value (which
     * may be {@code null}) from a cache miss (an empty {@link Optional}).
     *
     * @param value the cached value; may be null
     */
    record Hit(Object value) {
    }
}