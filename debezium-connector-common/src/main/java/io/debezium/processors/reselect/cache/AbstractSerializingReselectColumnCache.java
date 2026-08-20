/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect.cache;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.serde.ConnectValueSerde;

/**
 * Base class for {@link ReselectColumnCache} implementations backed by a byte-oriented store (embedded
 * key/value store, remote cache, object store). Subclasses implement only the byte-level storage
 * primitives; this class owns the mapping between the SPI's live Kafka Connect objects and stable bytes.
 * <p>
 * <b>Row key encoding.</b> The SPI's row identity is the event's message key {@link Struct}, whose
 * {@code equals} contract makes the key <em>schema</em> part of the identity: a DDL change or primary-key
 * reordering yields a different key and a natural cache miss rather than a false hit against a stale
 * entry. To preserve those semantics in a byte-keyed store, the row key is encoded via
 * {@link ConnectValueSerde#serializeStructIdentity(Struct)} as a fingerprint of the key schema followed by the
 * key's field values written positionally (see that method for details). The per-column storage key
 * appends a {@code 0x00} separator and the UTF-8 column name, so all columns of a row share a common byte
 * prefix and sort adjacently in ordered stores.
 * <p>
 * <b>Value encoding.</b> Values are serialized via {@link ConnectValueSerde}: a versioned envelope
 * carrying the write timestamp (for read-side TTL enforcement) and a type-tagged, value-only payload.
 * Values whose type the serde does not support are skipped with a one-time warning; the column is simply
 * re-queried on a later miss. Undecodable stored bytes (e.g. after a format change) are deleted and
 * treated as a miss.
 * <p>
 * <b>TTL.</b> {@code reselect.cache.ttl.ms} defaults to {@code 0} (no expiration) here: persistence is
 * the point of a durable backend, and correctness never depends on the TTL because the post-processor
 * refreshes entries on modification. This deliberately differs from {@link MemoryReselectColumnCache},
 * whose TTL defaults to ten minutes to bound heap retention. When a positive TTL is configured, expiry is
 * enforced strictly at read time against the envelope's write timestamp (expired entries are deleted and
 * reported as misses); subclasses may additionally reclaim expired data in storage.
 * <p>
 * <b>Storage failures.</b> The cache is an optimization, so a failing backend must never fail event
 * processing: exceptions from the storage primitives are caught here and degraded, a read failure to a
 * cache miss (the column falls back to re-selection) and a write or removal failure to a skipped
 * operation, each with a warning. Subclasses may therefore let their storage exceptions propagate freely.
 * <p>
 * Storage primitives are invoked concurrently (streaming and snapshot threads) and must be thread-safe.
 *
 * @author Chris Cranford
 */
@Incubating
public abstract class AbstractSerializingReselectColumnCache implements ReselectColumnCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSerializingReselectColumnCache.class);

    public static final Field TTL_MS = Field.create("reselect.cache.ttl.ms")
            .withDisplayName("Reselect cache TTL")
            .withType(ConfigDef.Type.LONG)
            .withDefault(0L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Time-to-live in milliseconds for cached values; 0 disables expiration so "
                    + "entries persist until overwritten or invalidated.");

    private static final byte KEY_COLUMN_SEPARATOR = 0x00;

    private final Set<String> unsupportedValueTypes = ConcurrentHashMap.newKeySet();
    private final Set<String> unsupportedKeyTypes = ConcurrentHashMap.newKeySet();

    private ConnectValueSerde serde;
    private long ttlMs;

    @Override
    public void configure(Configuration config) {
        this.ttlMs = config.getLong(TTL_MS);
        this.serde = new ConnectValueSerde();
        configureStorage(config);
    }

    @Override
    public RowCache forRow(Struct messageKey) {
        final byte[] rowKey;
        try {
            rowKey = serde.serializeStructIdentity(messageKey);
        }
        catch (Exception e) {
            // An unencodable key disables caching for this row only; processing must not fail because of it.
            final String keyType = messageKey.schema().name();
            if (unsupportedKeyTypes.add(String.valueOf(keyType))) {
                LOGGER.warn("Unable to encode reselect cache row key for key schema '{}'; caching is skipped for such rows.", keyType, e);
            }
            return NoOpRowCache.INSTANCE;
        }
        return new SerializingRowCache(rowKey);
    }

    /**
     * Configure the underlying storage. Invoked once from {@link #configure(Configuration)} after the
     * common cache options have been read.
     *
     * @param config the connector configuration, including any storage-specific properties
     */
    protected abstract void configureStorage(Configuration config);

    /**
     * Return the stored bytes for the given storage key, or {@code null} if absent. Must be thread-safe.
     * May throw on storage failure; the failure is degraded to a cache miss.
     */
    protected abstract byte[] getFromStorage(byte[] key);

    /**
     * Store the given bytes under the given storage key. Must be thread-safe. May throw on storage
     * failure; the failure is degraded to a skipped write, so the value is re-queried on a later miss.
     */
    protected abstract void putToStorage(byte[] key, byte[] value);

    /**
     * Remove the entry for the given storage key, if any. Must be thread-safe. May throw on storage
     * failure; the failure is degraded to a skipped removal.
     */
    protected abstract void removeFromStorage(byte[] key);

    /**
     * The configured time-to-live in milliseconds; {@code 0} when expiration is disabled.
     */
    protected long getTtlMs() {
        return ttlMs;
    }

    /**
     * The current wall-clock time; overridable by tests to exercise TTL behavior deterministically.
     */
    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    private final class SerializingRowCache implements RowCache {

        private final byte[] rowKey;

        private SerializingRowCache(byte[] rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public Optional<Hit> get(String column) {
            final byte[] storageKey = storageKey(column);
            final byte[] stored;
            try {
                stored = getFromStorage(storageKey);
            }
            catch (Exception e) {
                // The cache must never fail event processing; a failed read is a miss and the column
                // falls back to re-selection.
                LOGGER.warn("Reselect cache read failed for column '{}'; falling back to re-selection.", column, e);
                return Optional.empty();
            }
            if (stored == null) {
                return Optional.empty();
            }
            final ConnectValueSerde.DeserializedValue deserialized;
            try {
                deserialized = serde.deserialize(stored);
            }
            catch (Exception e) {
                LOGGER.warn("Discarding undecodable reselect cache entry for column '{}'.", column, e);
                tryRemoveFromStorage(storageKey, column);
                return Optional.empty();
            }
            if (ttlMs > 0 && currentTimeMillis() - deserialized.timestampMs() >= ttlMs) {
                tryRemoveFromStorage(storageKey, column);
                return Optional.empty();
            }
            return Optional.of(new Hit(deserialized.value()));
        }

        @Override
        public void put(String column, Schema schema, Object value) {
            final byte[] serialized;
            try {
                serialized = serde.serialize(value, schema, currentTimeMillis());
            }
            catch (Exception e) {
                final String valueType = value != null ? value.getClass().getName() : "null";
                if (unsupportedValueTypes.add(valueType)) {
                    LOGGER.warn("Values of type '{}' cannot be serialized and will not be cached; they will be re-queried on demand.", valueType, e);
                }
                return;
            }
            try {
                putToStorage(storageKey(column), serialized);
            }
            catch (Exception e) {
                // A failed write only skips caching; the value is re-queried on a later miss.
                LOGGER.warn("Reselect cache write failed for column '{}'; the value will be re-queried on demand.", column, e);
            }
        }

        @Override
        public void invalidate(String column) {
            tryRemoveFromStorage(storageKey(column), column);
        }

        private void tryRemoveFromStorage(byte[] storageKey, String column) {
            try {
                removeFromStorage(storageKey);
            }
            catch (Exception e) {
                LOGGER.warn("Reselect cache removal failed for column '{}'.", column, e);
            }
        }

        private byte[] storageKey(String column) {
            final byte[] columnBytes = column.getBytes(StandardCharsets.UTF_8);
            final byte[] storageKey = Arrays.copyOf(rowKey, rowKey.length + 1 + columnBytes.length);
            storageKey[rowKey.length] = KEY_COLUMN_SEPARATOR;
            System.arraycopy(columnBytes, 0, storageKey, rowKey.length + 1, columnBytes.length);
            return storageKey;
        }
    }

    private enum NoOpRowCache implements RowCache {
        INSTANCE;

        @Override
        public Optional<Hit> get(String column) {
            return Optional.empty();
        }

        @Override
        public void put(String column, Schema schema, Object value) {
        }

        @Override
        public void invalidate(String column) {
        }
    }

}