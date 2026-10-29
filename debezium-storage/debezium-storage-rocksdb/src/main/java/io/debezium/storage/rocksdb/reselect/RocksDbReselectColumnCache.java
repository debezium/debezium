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
 *       an SST file, reclaiming expired entries sooner; 0 keeps RocksDB's default and does not disable
 *       periodic compaction (default: 0)</li>
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
        // 0 (the default) means the setter is not called, so RocksDB applies its own default; it does
        // not disable periodic compaction as an explicit RocksDB periodic_compaction_seconds=0 would.
        final long periodicCompactionSeconds = config.getLong(PERIODIC_COMPACTION_SECONDS, 0L);
        if (periodicCompactionSeconds < 0) {
            throw new DebeziumException(String.format("Configuration property '%s' must not be negative, but was %d",
                    PERIODIC_COMPACTION_SECONDS, periodicCompactionSeconds));
        }

        try {
            Files.createDirectories(dbPath);
            try (Options options = RocksDbUtil.createOptions()) {
                if (periodicCompactionSeconds > 0) {
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
                    dbPath, getTtlMs(), cleanupOnClose, periodicCompactionSeconds > 0 ? periodicCompactionSeconds + " s" : "RocksDB default");
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
}