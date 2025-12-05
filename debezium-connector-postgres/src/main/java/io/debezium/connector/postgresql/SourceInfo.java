/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.relational.TableId;

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
public final class SourceInfo extends BaseSourceInfo {

    public static final String TIMESTAMP_USEC_KEY = "ts_usec";
    public static final String TXID_KEY = "txId";
    public static final String XMIN_KEY = "xmin";
    public static final String LSN_KEY = "lsn";
    public static final String MSG_TYPE_KEY = "messageType";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String dbName;

    private Lsn lsn;
    private Lsn lastCommitLsn;
    private Long txId;
    private Long xmin;
    private Operation messageType;
    private Instant timestamp;
    private String schemaName;
    private String tableName;

    protected SourceInfo(PostgresConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.dbName = connectorConfig.databaseName();
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param lsn the position in the server WAL for a particular event; may be null indicating that this information is not
     * available
     * @param commitTime the commit time of the transaction that generated the event;
     * may be null indicating that this information is not available
     * @param txId the ID of the transaction that generated the transaction; may be null if this information is not available
     * @param xmin the xmin of the slot, may be null
     * @param tableId the table that should be included in the source info; may be null
     * @param messageType the type of the message corresponding to the lsn; may be null
     * @return this instance
     */
    protected SourceInfo update(Lsn lsn, Instant commitTime, Long txId, Long xmin, TableId tableId, Operation messageType) {
        this.lsn = lsn;
        if (commitTime != null) {
            this.timestamp = commitTime;
        }
        this.txId = txId;
        this.xmin = xmin;
        if (messageType != null) {
            this.messageType = messageType;
        }
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        else {
            this.schemaName = "";
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        else {
            this.tableName = "";
        }
        return this;
    }

    protected SourceInfo update(Instant timestamp, TableId tableId) {
        this.timestamp = timestamp;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    /**
     * Updates the source with the LSN of the last committed transaction.
     */
    protected SourceInfo updateLastCommit(Lsn lsn) {
        this.lastCommitLsn = lsn;
        return this;
    }

    public Lsn lsn() {
        return this.lsn;
    }

    public Long xmin() {
        return this.xmin;
    }

    public Operation messageType() {
        return this.messageType;
    }

    @Override
    public String sequence() {
        List<String> sequence = new ArrayList<String>(2);
        String lastCommitLsn = (this.lastCommitLsn != null)
                ? Long.toString(this.lastCommitLsn.asLong())
                : null;
        String lsn = (this.lsn != null)
                ? Long.toString(this.lsn.asLong())
                : null;
        sequence.add(lastCommitLsn);
        sequence.add(lsn);
        try {
            return MAPPER.writeValueAsString(sequence);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
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

    protected Long txId() {
        return txId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("source_info[");
        sb.append("server='").append(serverName()).append('\'');
        sb.append("db='").append(dbName).append('\'');
        if (lsn != null) {
            sb.append(", lsn=").append(lsn);
        }
        if (txId != null) {
            sb.append(", txId=").append(txId);
        }
        if (xmin != null) {
            sb.append(", xmin=").append(xmin);
        }
        if (messageType != null) {
            sb.append(", messageType=").append(messageType);
        }
        if (lastCommitLsn != null) {
            sb.append(", lastCommitLsn=").append(lastCommitLsn);
        }
        if (timestamp != null) {
            sb.append(", timestamp=").append(timestamp);
        }
        sb.append(", snapshot=").append(snapshotRecord);
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
