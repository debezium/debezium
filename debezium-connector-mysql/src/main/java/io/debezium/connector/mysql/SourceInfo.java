/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.relational.TableId;

/**
 * Information about the source of information, which includes the position in the source binary log we have previously processed.
 * <p>
 * The {@link MySqlPartition#getSourcePartition() source partition} information describes the database whose log is being consumed. Typically, the
 * database is identified by the host address port number of the MySQL server and the name of the database. Here's a JSON-like
 * representation of an example database:
 *
 * <pre>
 * {
 *     "server" : "production-server"
 * }
 * </pre>
 *
 * <p>
 * The offset includes the {@link #binlogFilename() binlog filename}, the {@link #binlogPosition() position of the first event} in the binlog, the
 * {@link MySqlOffsetContext#eventsToSkipUponRestart() number of events to skip}, and the
 * {@link MySqlOffsetContext#rowsToSkipUponRestart() number of rows to also skip}.
 * <p>
 * Here's a JSON-like representation of an example offset:
 *
 * <pre>
 * {
 *     "server_id": 112233,
 *     "ts_sec": 1465937,
 *     "gtid": "db58b0ae-2c10-11e6-b284-0242ac110002:199",
 *     "file": "mysql-bin.000003",
 *     "pos" = 990,
 *     "event" = 0,
 *     "row": 0,
 *     "snapshot": true
 * }
 * </pre>
 * <p>
 * The "{@code gtids}" field only appears in offsets produced when GTIDs are enabled. The "{@code snapshot}" field only appears in
 * offsets produced when the connector is in the middle of a snapshot. And finally, the "{@code ts}" field contains the
 * <em>seconds</em> since Unix epoch (since Jan 1, 1970) of the MySQL event; the message {@link Envelope envelopes} also have a
 * timestamp, but that timestamp is the <em>milliseconds</em> since since Jan 1, 1970.
 * <p>
 * Each change event envelope also includes the {@link #struct() source} struct that contains MySQL information about that
 * particular event, including a mixture the fields from the binlog filename and position where the event can be found,
 * and when GTIDs are enabled the GTID of the
 * transaction in which the event occurs. Like with the offset, the "{@code snapshot}" field only appears for events produced
 * when the connector is in the middle of a snapshot. Note that this information is likely different than the offset information,
 * since the connector may need to restart from either just after the most recently completed transaction or the beginning
 * of the most recently started transaction (whichever appears later in the binlog).
 * <p>
 * Here's a JSON-like representation of the source for the metadata for an event that corresponds to the above partition and
 * offset:
 *
 * <pre>
 * {
 *     "name": "production-server",
 *     "server_id": 112233,
 *     "ts_sec": 1465937,
 *     "gtid": "db58b0ae-2c10-11e6-b284-0242ac110002:199",
 *     "file": "mysql-bin.000003",
 *     "pos" = 1081,
 *     "row": 0,
 *     "snapshot": true,
 *     "thread" : 1,
 *     "db" : "inventory",
 *     "table" : "products"
 * }
 * </pre>
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public final class SourceInfo extends BaseSourceInfo {

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

    public SourceInfo(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);

        this.tableIds = new HashSet<>();
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
     * Set the position in the MySQL binlog where we will start reading.
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
     * Set the position within the MySQL binary log file of the <em>current event</em>.
     *
     * @param positionOfCurrentEvent the position within the binary log file of the current event
     */
    public void setEventPosition(long positionOfCurrentEvent) {
        this.currentBinlogPosition = positionOfCurrentEvent;
    }

    /**
     * Given the row number within a binlog event and the total number of rows in that event, compute the
     * Kafka Connect offset that is be included in the produced change event describing the row.
     * <p>
     * This method should always be called before {@link #struct()}.
     *
     * @param eventRowNumber the 0-based row number within the event for which the offset is to be produced
     * @see #struct()
     */
    public void setRowNumber(int eventRowNumber) {
        this.currentRowNumber = eventRowNumber;
    }

    public void databaseEvent(String databaseName) {
        this.databaseName = databaseName;
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    public void startGtid(String gtid) {
        this.currentGtid = gtid;
    }

    /**
     * Set the server ID as found within the MySQL binary log file.
     *
     * @param serverId the server ID found within the binary log file
     */
    public void setBinlogServerId(long serverId) {
        this.serverId = serverId;
    }

    /**
     * Set the number of <em>seconds</em> since Unix epoch (January 1, 1970) as found within the MySQL binary log file.
     * Note that the value in the binlog events is in seconds, but the library we use returns the value in milliseconds
     * (with only second precision and therefore all fractions of a second are zero). We capture this as seconds
     * since that is the precision that MySQL uses.
     *
     * @param timestampInSeconds the timestamp in <em>seconds</em> found within the binary log file
     */
    public void setBinlogTimestampSeconds(long timestampInSeconds) {
        this.sourceTime = Instant.ofEpochSecond(timestampInSeconds);
    }

    public void setSourceTime(Instant timestamp) {
        sourceTime = timestamp;
    }

    /**
     * Set the identifier of the MySQL thread that generated the most recent event.
     *
     * @param threadId the thread identifier; may be negative if not known
     */
    public void setBinlogThread(long threadId) {
        this.threadId = threadId;
    }

    /**
     * Get the name of the MySQL binary log file that has last been processed.
     *
     * @return the name of the binary log file; null if it has not been {@link MySqlOffsetContext#setBinlogStartPoint(String, long) set}
     */
    public String binlogFilename() {
        return currentBinlogFilename;
    }

    /**
     * Get the position within the MySQL binary log file of the next event to be processed.
     *
     * @return the position within the binary log file; null if it has not been {@link MySqlOffsetContext#setBinlogStartPoint(String, long) set}
     */
    public long binlogPosition() {
        return currentBinlogPosition;
    }

    long getServerId() {
        return serverId;
    }

    long getThreadId() {
        return threadId;
    }

    /**
     * Returns a string representation of the table(s) affected by the current
     * event. Will only represent more than a single table for events in the
     * user-facing schema history topic for certain types of DDL. Will be {@code null}
     * for DDL events not applying to tables (CREATE DATABASE etc.).
     */
    String table() {
        return tableIds.isEmpty() ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }

    String getCurrentGtid() {
        return currentGtid;
    }

    String getCurrentBinlogFilename() {
        return currentBinlogFilename;
    }

    long getCurrentBinlogPosition() {
        return currentBinlogPosition;
    }

    long getBinlogTimestampSeconds() {
        return (sourceTime == null) ? 0 : sourceTime.getEpochSecond();
    }

    int getCurrentRowNumber() {
        return currentRowNumber;
    }

    @Override
    public String toString() {
        return "SourceInfo [currentGtid=" + currentGtid + ", currentBinlogFilename=" + currentBinlogFilename
                + ", currentBinlogPosition=" + currentBinlogPosition + ", currentRowNumber=" + currentRowNumber
                + ", serverId=" + serverId + ", sourceTime=" + sourceTime + ", threadId=" + threadId + ", currentQuery="
                + currentQuery + ", tableIds=" + tableIds + ", databaseName=" + databaseName + "]";
    }

    /**
     * Create a {@link Document} from the given offset.
     *
     * @param offset the offset to create the document from.
     * @return a {@link Document} with the offset data.
     */
    public static Document createDocumentFromOffset(Map<String, ?> offset) {
        Document offsetDocument = Document.create();
        // all of the offset keys represent int, long, or string types, so we don't need to worry about references
        // and information changing underneath us.

        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetDocument.set(entry.getKey(), entry.getValue());
        }

        return offsetDocument;
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
}
