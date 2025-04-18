/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.document.Document;
import io.debezium.relational.TableId;

/**
 * Information about the source, including the position in the source binary log we have processed.<p></p>
 *
 * The {@link BinlogPartition#getSourcePartition()} information describes the database whose log is currently
 * being processed. Typically, the database is identified by the host address port of the database server and
 * the name of the database. Here's a JSON representation of an example database:
 *
 * <pre>
 *     {
 *         "server": "production-server"
 *     }
 * </pre>
 *
 * The offset includes the {@link #binlogFilename()} and the {@link #binlogPosition()}, along with details on how many
 * {@link BinlogOffsetContext#eventsToSkipUponRestart()} and the {@link BinlogOffsetContext#rowsToSkipUponRestart()}.
 * Another JSON example:
 *
 * <pre>
 *     {
 *         "server_id": 112233,
 *         "ts_sec": 1234567,
 *         "gtid": "<binary-log-database-formatted-gtid>",
 *         "file": "mysql-bin.000003",
 *         "pos": 990,
 *         "event": 0,
 *         "row": 0,
 *         "snapshot": true
 *     }
 * </pre>
 *
 * The "{@code gtid}" field only appears in offsets produced when GTIDs are enabled. The "@{code snapshot}" field
 * only appears in offsets produced when the connector is in the middle of a snapshot. And finally, the "{@code ts}"
 * field contains the seconds since the Unix epoch for the database event; the message {@link io.debezium.data.Envelope}
 * also has a timestamp, but that timestamp is in milliseconds since the Unix epoch.<p></p>
 *
 * Each change event envelope also includes the {@link #struct()} that contains database information about that
 * specific event, including a mixture of the fields from the binlog filename and position where the event can
 * be found, and when GTIDs are enabled, the GTID of the transaction in which the event occurs. Like with the
 * offset, the "{@code snapshot}" field only appears for events produced when the connector is in the middle of
 * a snapshot. Note that this information is likely different from the offset information, since the connector
 * may need to restart from either just after the most recently completed transaction or the beginning of the
 * most recently started transaction (whichever appears later in the binlog).<p></p>
 *
 * Here's a JSON representation of the source metadata for an event that represents the partition and offset:
 *
 * <pre>
 *     {
 *         "name": "production-server",
 *         "server_id": 112233,
 *         "ts_sec", 1234567,
 *         "gtid": "<binary-log-database-formatted-gtid>",
 *         "file": "mysql-bin.000003",
 *         "pos": 1081,
 *         "row": 0,
 *         "snapshot": true,
 *         "thread": 1,
 *         "db": "inventory",
 *         "table": "products"
 *     }
 * </pre>
 *
 * @author Randall Hauch
 * @author Chris Cranford
 */
@NotThreadSafe
public class BinlogSourceInfo extends BaseSourceInfo {

    // Avro Schema doesn't allow "-" to be included as field name, use "_" instead.
    // Ref https://issues.apache.org/jira/browse/AVRO-838.
    public static final String SERVER_ID_KEY = "server_id";

    public static final String GTID_KEY = "gtid";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
    public static final String THREAD_KEY = "thread";
    public static final String QUERY_KEY = "query";

    private String currentGtid;
    private String currentBinlogFilename;
    private long currentBinlogPosition = 0L;
    private int currentRowNumber = 0;
    private long serverId = 0;
    private Instant sourceTime = null;
    private long threadId = -1L;
    private String currentQuery = null;
    private Set<TableId> tableIds;
    private String databaseName;

    public BinlogSourceInfo(BinlogConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.tableIds = new HashSet<>();
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        if (tableIds == null || tableIds.isEmpty()) {
            return databaseName;
        }
        final TableId tableId = tableIds.iterator().next();
        if (tableId == null) {
            return databaseName;
        }
        return tableId.catalog();
    }

    /**
     * Set the original SQL query.
     *
     * @param query the original SQL query that generated the event.
     */
    public void setQuery(final String query) {
        this.currentQuery = query;
    }

    /**
     * @return the original SQL query that generated the event.  NULL if no such query is associated.
     */
    public String getQuery() {
        return this.currentQuery;
    }

    /**
     * Set the position in the binlog where we will start reading.
     *
     * @param binlogFilename the name of the binary log file; may not be null
     * @param positionOfFirstEvent the position in the binary log file to begin processing
     */
    public void setBinlogPosition(String binlogFilename, long positionOfFirstEvent) {
        if (binlogFilename != null) {
            this.currentBinlogFilename = binlogFilename;
        }
        assert positionOfFirstEvent >= 0;
        this.currentBinlogPosition = positionOfFirstEvent;
        this.currentRowNumber = 0;
    }

    /**
     * Set the position within the binary log file of the <em>current event</em>.
     *
     * @param positionOfCurrentEvent the position within the binary log file of the current event
     */
    public void setEventPosition(long positionOfCurrentEvent) {
        this.currentBinlogPosition = positionOfCurrentEvent;
    }

    /**
     * Given the row number within a binlog event and the total number of rows in that event, compute the
     * Kafka Connect offset that is to be included in the produced change event describing the row.<p></p>
     *
     * This method should always be called before {@link #struct()}.
     *
     * @param eventRowNumber the 0-based row number within the event for which the offset is to be produced
     * @see #struct()
     */
    public void setRowNumber(int eventRowNumber) {
        this.currentRowNumber = eventRowNumber;
    }

    /**
     * Set the database that the <em>current</em> binlog event represents.
     *
     * @param databaseName the database name
     */
    public void databaseEvent(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Sets the associated table identifiers that the <em>current</em> binlog event represents.
     *
     * @param tableIds set of table ids, should not be null but may be empty
     */
    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    /**
     * Sets the associate table identifier that the <em>current</em> binlog event represents.
     *
     * @param tableId the table id, should not be null
     */
    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    /**
     * Set the starting global transaction identifier
     *
     * @param gtid the global transaction identifier
     */
    public void startGtid(String gtid) {
        this.currentGtid = gtid;
    }

    /**
     * Set the server ID as found within the binary log file.
     *
     * @param serverId the server ID found within the binary log file
     */
    public void setBinlogServerId(long serverId) {
        this.serverId = serverId;
    }

    /**
     * Set the instant from the binlog log file for the specified event.
     *
     * @param timestamp the time the binlog event occurred
     */
    public void setSourceTime(Instant timestamp) {
        sourceTime = timestamp;
    }

    /**
     * Set the identifier of the binlog thread that generated the most recent event.
     *
     * @param threadId the thread identifier; may be negative if not known
     */
    public void setBinlogThread(long threadId) {
        this.threadId = threadId;
    }

    /**
     * Get the name of the binary log file that has last been processed.
     *
     * @return the name of the binary log file; null if it has not been set
     */
    public String binlogFilename() {
        return currentBinlogFilename;
    }

    /**
     * Get the position within the binary log file of the next event to be processed.
     *
     * @return the position within the binary log file; zero if it has not been set
     */
    public long binlogPosition() {
        return currentBinlogPosition;
    }

    /**
     * Get the server ID
     *
     * @return the server ID within the binary log file; 0 if it has not been set
     */
    public long getServerId() {
        return serverId;
    }

    /**
     * Get the identifier of the binlog thread
     *
     * @return the binary log thread identifier; -1 if it has not been set
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * Returns a string representation of the table(s) affected by the current event. Will only represent
     * more than a single table for events in the user-facing schema history topic for certain types of
     * DDL events. Will be {@code null} for DDL events not applying to tables, i.e. CREATE DATABASE.
     */
    public String table() {
        return tableIds.isEmpty() ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }

    /**
     * Get the current recorded global transaction identifier (GTID).
     *
     * @return the current global transaction identifier (GTID); will be {@code null} if GTID is not enabled
     */
    public String getCurrentGtid() {
        return currentGtid;
    }

    /**
     * Get the current binlog file being processed.
     *
     * @return the current binlog file being processed; null if it has not been set
     */
    public String getCurrentBinlogFilename() {
        return currentBinlogFilename;
    }

    /**
     * Get the current binlog file position.
     *
     * @return the current binlog file position; 0 if it has not been set
     */
    public long getCurrentBinlogPosition() {
        return currentBinlogPosition;
    }

    /**
     * Returns the computed row number within a binlog event.
     *
     * @return the computed binlog event row number; 0 if it has not been set
     */
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    @Override
    public String toString() {
        return "BinlogSourceInfo{" +
                "currentGtid='" + currentGtid + '\'' +
                ", currentBinlogFilename='" + currentBinlogFilename + '\'' +
                ", currentBinlogPosition=" + currentBinlogPosition +
                ", currentRowNumber=" + currentRowNumber +
                ", serverId=" + serverId +
                ", sourceTime=" + sourceTime +
                ", threadId=" + threadId +
                ", currentQuery='" + currentQuery + '\'' +
                ", tableIds=" + tableIds +
                ", databaseName='" + databaseName + '\'' +
                "}";
    }

    /**
     * Create a {@link Document} from the given offset.
     *
     * @param offset the offset to create the document from.
     * @return a {@link Document} with the offset data.
     */
    public static Document createDocumentFromOffset(Map<String, ?> offset) {
        final Document offsetDocument = Document.create();

        // All the offset keys represent int, long, or string types, so we don't need to worry about references
        // and information changing underneath us.
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetDocument.set(entry.getKey(), entry.getValue());
        }

        return offsetDocument;
    }
}
