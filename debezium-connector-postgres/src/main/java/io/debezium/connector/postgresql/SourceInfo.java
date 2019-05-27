/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.relational.TableId;
import io.debezium.time.Conversions;

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
public final class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_PARTITION_KEY = "server";
    public static final String DB_NAME_KEY = "db";
    public static final String TIMESTAMP_USEC_KEY = "ts_usec";
    public static final String TXID_KEY = "txId";
    public static final String XMIN_KEY = "xmin";
    public static final String LSN_KEY = "lsn";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    private final String dbName;
    private final Map<String, String> sourcePartition;

    private Long lsn;
    private Long txId;
    private Long xmin;
    private Instant timestamp;
    private boolean snapshot = false;
    private Boolean lastSnapshotRecord;
    private String schemaName;
    private String tableName;
    private final SourceInfoStructMaker<SourceInfo> structMaker;

    protected SourceInfo(PostgresConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.dbName = connectorConfig.databaseName();
        this.sourcePartition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());

        this.structMaker = connectorConfig.getSourceInfoStructMaker(SourceInfo.class);
    }

    protected void load(Map<String, Object> lastStoredOffset) {
        this.lsn = ((Number) lastStoredOffset.get(LSN_KEY)).longValue();
        this.txId = ((Number) lastStoredOffset.get(TXID_KEY)).longValue();
        this.xmin = (Long) lastStoredOffset.get(XMIN_KEY);
        this.timestamp = Conversions.toInstantFromMicros((Long) lastStoredOffset.get(TIMESTAMP_USEC_KEY));
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
        assert serverName() != null && dbName != null;
        Map<String, Object> result = new HashMap<>();
        if (timestamp != null) {
            result.put(TIMESTAMP_USEC_KEY, Conversions.toEpochMicros(timestamp));
        }
        if (txId != null) {
            result.put(TXID_KEY, txId);
        }
        if (lsn != null) {
            result.put(LSN_KEY, lsn);
        }
        if (xmin != null) {
            result.put(XMIN_KEY, xmin);
        }
        if (snapshot) {
            result.put(SNAPSHOT_KEY, true);
            result.put(LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        return result;
    }

    public OffsetState asOffsetState() {
        return new OffsetState(lsn, txId, xmin, timestamp, isSnapshotInEffect());
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param lsn the position in the server WAL for a particular event; may be null indicating that this information is not
     * available
     * @param commitTime the commit time of the transaction that generated the event;
     * may be null indicating that this information is not available
     * @param txId the ID of the transaction that generated the transaction; may be null if this information is not available
     * @param tableId the table that should be included in the source info; may be null
     * @param xmin the xmin of the slot, may be null
     * @return this instance
     */
    protected SourceInfo update(Long lsn, Instant commitTime, Long txId, TableId tableId, Long xmin) {
        this.lsn = lsn;
        if (commitTime != null) {
            this.timestamp = commitTime;
        }
        this.txId = txId;
        this.xmin = xmin;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    protected SourceInfo update(Instant useconds, TableId tableId) {
        this.timestamp = useconds;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    protected SourceInfo markLastSnapshotRecord() {
        this.lastSnapshotRecord = true;
        return this;
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition()} and {@link #offset()} information. The Struct
     * complies with the {@link PostgresSourceInfoStructMaker#schema()} for the Postgres connector.
     * <p>
     * This method should always be called after {@link #update(Long, Instant, Long, TableId, Long)}.
     *
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    protected Struct source() {
        return structMaker.struct(this);
    }

    /**
     * Determine whether a snapshot is currently in effect, meaning it was started and has not completed.
     *
     * @return {@code true} if a snapshot is in effect, or {@code false} otherwise
     */
    public boolean isSnapshotInEffect() {
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

    public Long lsn() {
        return this.lsn;
    }

    public Long xmin() {
        return this.xmin;
    }

    @Override
    protected String database() {
        return dbName;
    }

    String schemaName() {
        return schemaName;
    }

    String tableName() {
        return tableName;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected boolean snapshot() {
        return snapshot;
    }

    public boolean hasLastKnownPosition() {
        return this.lsn != null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("source_info[");
        sb.append("server='").append(serverName()).append('\'');
        sb.append("db='").append(dbName).append('\'');
        if (lsn != null) {
            sb.append(", lsn=").append(ReplicationConnection.format(lsn));
        }
        if (txId != null) {
            sb.append(", txId=").append(txId);
        }
        if (xmin != null) {
            sb.append(", xmin=").append(xmin);
        }
        if (timestamp != null) {
            sb.append(", useconds=").append(timestamp);
        }
        boolean snapshotInEffect = isSnapshotInEffect();
        sb.append(", snapshot=").append(snapshotInEffect);
        if (snapshotInEffect) {
            sb.append(", last_snapshot_record=").append(lastSnapshotRecord);
        }
        if (schemaName != null) {
            sb.append(", schema=").append(schemaName);
        }
        if (tableName != null) {
            sb.append(", table=").append(tableName);
        }
        sb.append(']');
        return sb.toString();
    }
}
