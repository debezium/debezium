/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.reselect;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.processors.reselect.cache.AbstractSerializingReselectColumnCache;
import io.debezium.storage.rocksdb.RocksDbUtil;
import io.debezium.util.Strings;

/**
 * A RocksDB-backed, disk-persisted reselect column cache. Cached values survive connector restarts, so
 * columns that would otherwise be re-queried after every restart (e.g. TOAST/LOB placeholders) can be
 * served from disk. Entries remain correct across restarts because the post-processor refreshes the cache
 * whenever a column arrives with a real value.
 * <p>
 * Storage is permanent by default: no TTL is applied and the database directory is retained on close.
 * A positive {@code reselect.cache.ttl.ms} enables expiration, enforced strictly at read time by the
 * serializing base class, with RocksDB's TTL support additionally reclaiming expired entries from disk
 * during compaction.
 * <p>
 * RocksDB takes an exclusive lock on its directory, so each connector (and each task of a multi-task
 * connector) must be configured with its own {@code reselect.cache.rocksdb.path}.
 *
 * <p>Configuration properties (relative to the post-processor's {@code post.processors.<name>.} prefix):
 * <ul>
 *   <li>{@code reselect.cache.rocksdb.path} - Directory path for RocksDB storage (required)</li>
 *   <li>{@code reselect.cache.rocksdb.cleanup} - Whether to delete the RocksDB files on close (default: false)</li>
 *   <li>{@code reselect.cache.ttl.ms} - Time-to-live for cached values; 0 disables expiration (default: 0)</li>
 *   <li>{@code reselect.cache.rocksdb.periodic.compaction.seconds} - Age in seconds after which RocksDB compacts
 *       an SST file, reclaiming expired entries sooner; 0 disables periodic compaction (default: not set,
 *       RocksDB's own default applies)</li>
 * </ul>
 *
 * @author Chris Cranford
 */
@Incubating
public class RocksDbReselectColumnCache extends AbstractSerializingReselectColumnCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbReselectColumnCache.class);

    static {
        RocksDB.loadLibrary();
    }

    public static final String PATH = "reselect.cache.rocksdb.path";
    public static final String CLEANUP = "reselect.cache.rocksdb.cleanup";
    public static final String PERIODIC_COMPACTION_SECONDS = "reselect.cache.rocksdb.periodic.compaction.seconds";

    private RocksDB db;
    private Path dbPath;
    private boolean cleanupOnClose;

    @Override
    protected void configureStorage(Configuration config) {
        final String configuredPath = config.getString(PATH);
        if (Strings.isNullOrEmpty(configuredPath)) {
            throw new DebeziumException(String.format("Configuration property '%s' is required but not set", PATH));
        }

        this.dbPath = Path.of(configuredPath);
        this.cleanupOnClose = config.getBoolean(CLEANUP, false);
        final Long periodicCompactionSeconds = getPeriodicCompactionSeconds(config);

        try {
            Files.createDirectories(dbPath);
            try (Options options = RocksDbUtil.createOptions()) {
                if (periodicCompactionSeconds != null) {
                    // Forces SST files older than this to be compacted, so TTL-expired entries (and deleted or
                    // overwritten values) are reclaimed from disk even when the file is otherwise never compacted.
                    options.setPeriodicCompactionSeconds(periodicCompactionSeconds);
                }
                final long ttlMs = getTtlMs();
                if (ttlMs > 0) {
                    // RocksDB's TTL reclaims expired entries during compaction but may still serve them
                    // until then; the strict expiry check lives in the base class's read path, so the disk
                    // TTL only needs to be a reclamation bound (rounded up to at least one second).
                    final int ttlSeconds = (int) Math.max(1, ttlMs / 1000);
                    db = TtlDB.open(options, dbPath.toString(), ttlSeconds, false);
                }
                else {
                    db = RocksDB.open(options, dbPath.toString());
                }
            }
            LOGGER.info("RocksDB reselect cache initialized at: {} (ttl: {} ms, cleanup on close: {}, periodic compaction: {})",
                    dbPath, getTtlMs(), cleanupOnClose, describePeriodicCompaction(periodicCompactionSeconds));
        }
        catch (IOException | RocksDBException e) {
            throw new DebeziumException("Failed to initialize RocksDB reselect cache at " + dbPath, e);
        }
    }

    @Override
    protected byte[] getFromStorage(byte[] key) {
        try {
            return db.get(key);
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to get value from RocksDB reselect cache", e);
        }
    }

    @Override
    protected void putToStorage(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to put value into RocksDB reselect cache", e);
        }
    }

    @Override
    protected void removeFromStorage(byte[] key) {
        try {
            db.delete(key);
        }
        catch (RocksDBException e) {
            throw new DebeziumException("Failed to remove value from RocksDB reselect cache", e);
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
        if (cleanupOnClose && dbPath != null) {
            RocksDbUtil.deleteDirectory(dbPath);
            LOGGER.info("RocksDB reselect cache cleaned up: {}", dbPath);
        }
    }

    /**
     * Parses {@link #PERIODIC_COMPACTION_SECONDS} strictly, so that a malformed value fails instead of
     * silently falling back to a default that might disable periodic compaction.
     * <p>
     * {@code null} (not set) means the setter is not called, so RocksDB applies its own default; {@code 0}
     * disables periodic compaction, just as RocksDB's own {@code periodic_compaction_seconds=0} does.
     *
     * @return the configured value, or {@code null} if the property is not set
     */
    private static Long getPeriodicCompactionSeconds(Configuration config) {
        final String value = config.getString(PERIODIC_COMPACTION_SECONDS);
        if (Strings.isNullOrBlank(value)) {
            return null;
        }
        final long seconds;
        try {
            seconds = Long.parseLong(value.trim());
        }
        catch (NumberFormatException e) {
            throw new DebeziumException(String.format("Configuration property '%s' must be a number of seconds, but was '%s'",
                    PERIODIC_COMPACTION_SECONDS, value), e);
        }
        if (seconds < 0) {
            throw new DebeziumException(String.format("Configuration property '%s' must not be negative, but was %d",
                    PERIODIC_COMPACTION_SECONDS, seconds));
        }
        return seconds;
    }

    private static String describePeriodicCompaction(Long periodicCompactionSeconds) {
        if (periodicCompactionSeconds == null) {
            return "RocksDB default";
        }
        return periodicCompactionSeconds == 0 ? "disabled" : periodicCompactionSeconds + " s";
    }
}