/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.postgresql.connection.ReplicationConnection;

/**
 * Information about the source of information, which for normal events contains information about the transaction id and the
 * LSN position in the server WAL.
 *
 * <p>
 * The {@link #partition() source partition} information describes the database server for which we're streaming changes.
 * Typically, the server is identified by the host address port number and the name of the database. Here's a JSON-like
 * representation of an example database:
 *
 * <pre>
 * {
 *     "server" : "production-server"
 * }
 * </pre>
 *
 * <p>
 * The {@link #offset() source offset} information describes a structure containing the position in the server's WAL for any
 * particular event, transaction id and the server timestamp at which the transaction that generated that particular event has
 * been committed. When performing snapshots, it may also contain a snapshot field which indicates that a particular record
 * is created while a snapshot it taking place.
 * Here's a JSON-like representation of an example:
 *
 * <pre>
 * {
 *     "ts_usec": 1465937,
 *     "lsn" : 99490,
 *     "txId" : 123,
 *     "snapshot": true
 * }
 * </pre>
 *
 * The "{@code ts_usec}" field contains the <em>microseconds</em> since Unix epoch (since Jan 1, 1970) representing the time at
 * which the transaction that generated the event was committed while the "{@code txId}" represents the server's unique transaction
 * identifier. The "{@code lsn}" field represent a numerical (long) value corresponding to the server's LSN for that particular
 * event and can be used to uniquely identify an event within the WAL.
 *
 * The {@link #source() source} struct appears in each message envelope and contains information about the event. It is
 * a mixture the fields from the {@link #partition() partition} and {@link #offset() offset}.
 * Like with the offset, the "{@code snapshot}" field only appears for events produced when the connector is in the
 * middle of a snapshot. Here's a JSON-like representation of the source for an event that corresponds to the above partition and
 * offset:
 *
 * <pre>
 * {
 *     "name": "production-server",
 *     "ts_usec": 1465937,
 *     "lsn" : 99490,
 *     "txId" : 123,
 *     "snapshot": true
 * }
 * </pre>
 *
 * @author Horia Chiorean
 */
@NotThreadSafe
final class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_NAME_KEY = "name";
    public static final String SERVER_PARTITION_KEY = "server";
    public static final String TIMESTAMP_KEY = "ts_usec";
    public static final String TXID_KEY = "txId";
    public static final String LSN_KEY = "lsn";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    /**
     * A {@link Schema} definition for a {@link Struct} used to store the {@link #partition()} and {@link #offset()} information.
     */
    public static final Schema SCHEMA = schemaBuilder()
                                                     .name("io.debezium.connector.postgresql.Source")
                                                     .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                                                     .field(TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                                                     .field(TXID_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                                                     .field(LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                                                     .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                                                     .field(LAST_SNAPSHOT_RECORD_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                                                     .build();

    private final String serverName;
    private final Map<String, String> sourcePartition;

    private Long lsn;
    private Integer txId;
    private Long useconds;
    private boolean snapshot = false;
    private Boolean lastSnapshotRecord;

    protected SourceInfo(String serverName) {
        super(Module.version());
        this.serverName = serverName;
        this.sourcePartition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
    }

    protected void load(Map<String, Object> lastStoredOffset) {
        this.lsn = ((Number) lastStoredOffset.get(LSN_KEY)).longValue();
        this.txId = ((Number) lastStoredOffset.get(TXID_KEY)).intValue();
        this.useconds = (Long) lastStoredOffset.get(TIMESTAMP_KEY);
        this.snapshot = lastStoredOffset.containsKey(SNAPSHOT_KEY);
        if (this.snapshot) {
            this.lastSnapshotRecord = (Boolean) lastStoredOffset.get(LAST_SNAPSHOT_RECORD_KEY);
        }
    }

    /**
     * Get the Kafka Connect detail about the source "partition", which describes the portion of the source that we are
     * consuming. Since we're streaming changes for a single database, the source partition specifies only the {@code serverName}
     * as the value for the partition.
     *
     * @return the source partition information; never null
     */
    public Map<String, String> partition() {
        return sourcePartition;
    }

    /**
     * Get the Kafka Connect detail about the source "offset", which describes the position within the source where we last
     * have last read.
     *
     * @return a copy of the current offset; never null
     */
    public Map<String, ?> offset() {
        assert serverName != null;
        Map<String, Object> result = new HashMap<>();
        if (useconds != null) {
            result.put(TIMESTAMP_KEY, useconds);
        }
        if (txId != null) {
            result.put(TXID_KEY, txId);
        }
        if (lsn != null) {
            result.put(LSN_KEY, lsn);
        }
        if (snapshot) {
            result.put(SNAPSHOT_KEY, true);
            result.put(LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        return result;
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param lsn the position in the server WAL for a particular event; may be null indicating that this information is not
     * available
     * @param useconds the commit time (in microseconds since epoch) of the transaction that generated the event;
     * may be null indicating that this information is not available
     * @param txId the ID of the transaction that generated the transaction; may be null if this information nis not available
     * @return this instance
     */
    protected SourceInfo update(Long lsn, Long useconds, Integer txId) {
        this.lsn = lsn;
        this.useconds = useconds;
        this.txId = txId;
        return this;
    }

    protected SourceInfo update(Long useconds) {
        this.useconds = useconds;
        return this;
    }

    protected SourceInfo markLastSnapshotRecord() {
        this.lastSnapshotRecord = true;
        return this;
    }

    /**
     * Get a {@link Schema} representation of the source {@link #partition()} and {@link #offset()} information.
     *
     * @return the source partition and offset {@link Schema}; never null
     * @see #source()
     */
    @Override
    protected Schema schema() {
        return SCHEMA;
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition()} and {@link #offset()} information. The Struct
     * complies with the {@link #SCHEMA} for the Postgres connector.
     * <p>
     * This method should always be called after {@link #update(Long, Long, Integer)}.
     *
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    protected Struct source() {
        assert serverName != null;
        Struct result = super.struct();
        result.put(SERVER_NAME_KEY, serverName);
        // use the offset information without the snapshot part (see below)
        offset().forEach(result::put);
        return result;
    }

    /**
     * Determine whether a snapshot is currently in effect, meaning it was started and has not completed.
     *
     * @return {@code true} if a snapshot is in effect, or {@code false} otherwise
     */
    protected boolean isSnapshotInEffect() {
        return snapshot && (this.lastSnapshotRecord == null || !this.lastSnapshotRecord);
    }

    /**
     * Denote that a snapshot is being (or has been) started.
     */
    protected void startSnapshot() {
        this.snapshot = true;
        this.lastSnapshotRecord = false;
    }

    /**
     * Denote that a snapshot has completed successfully.
     */
    protected void completeSnapshot() {
        this.snapshot = false;
    }

    protected Long lsn() {
        return this.lsn;
    }

    protected boolean hasLastKnownPosition() {
        return this.lsn != null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("source_info[");
        sb.append("server='").append(serverName).append('\'');
        if (lsn != null) {
            sb.append(", lsn=").append(ReplicationConnection.format(lsn));
        }
        if (txId != null) {
            sb.append(", txId=").append(txId);
        }
        if (useconds != null) {
            sb.append(", useconds=").append(useconds);
        }
        boolean snapshotInEffect = isSnapshotInEffect();
        sb.append(", snapshot=").append(snapshotInEffect);
        if (snapshotInEffect) {
            sb.append(", last_snapshot_record=").append(lastSnapshotRecord);
        }
        sb.append(']');
        return sb.toString();
    }
}
