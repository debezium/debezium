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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.relational.TableId;

/**
 * Information about the source of information, which includes the position in the source binary log we have previously processed.
 * <p>
 * The {@link MySqlOffsetContext#getPartition() source partition} information describes the database whose log is being consumed. Typically, the
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
 * The {@link MySqlOffsetContext#getOffset() source offset} information is included in each event and captures where the connector should restart
 * if this event's offset is the last one recorded. The offset includes the {@link #binlogFilename() binlog filename},
 * the {@link #binlogPosition() position of the first event} in the binlog, the
 * {@link #eventsToSkipUponRestart() number of events to skip}, and the
 * {@link #rowsToSkipUponRestart() number of rows to also skip}.
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
 * particular event, including a mixture the fields from the {@link MySqlOffsetContext#getPartition() partition} (which is renamed in the source to
 * make more sense), the binlog filename and position where the event can be found, and when GTIDs are enabled the GTID of the
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
final class SourceInfo extends BaseSourceInfo {

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
     * @return the name of the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    public String binlogFilename() {
        return currentBinlogFilename;
    }

    /**
     * Get the position within the MySQL binary log file of the next event to be processed.
     *
     * @return the position within the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
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
                        .filter(x -> x != null)
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

    /**
     * Determine whether the first {@link #offset() offset} is at or before the point in time of the second
     * offset, where the offsets are given in JSON representation of the maps returned by {@link #offset()}.
     * <p>
     * This logic makes a significant assumption: once a MySQL server/cluster has GTIDs enabled, they will
     * never be disabled. This is the only way to compare a position with a GTID to a position without a GTID,
     * and we conclude that any position with a GTID is *after* the position without.
     * <p>
     * When both positions have GTIDs, then we compare the positions by using only the GTIDs. Of course, if the
     * GTIDs are the same, then we also look at whether they have snapshots enabled.
     *
     * @param recorded the position obtained from recorded history; never null
     * @param desired the desired position that we want to obtain, which should be after some recorded positions,
     *            at some recorded positions, and before other recorded positions; never null
     * @param gtidFilter the predicate function that will return {@code true} if a GTID source is to be included, or
     *            {@code false} if a GTID source is to be excluded; may be null if no filtering is to be done
     * @return {@code true} if the recorded position is at or before the desired position; or {@code false} otherwise
     * 
     * Moved to {@link MySqlConnectorConfig#getHistoryRecordComparator}
     */
    @Deprecated
    public static boolean isPositionAtOrBefore(Document recorded, Document desired, Predicate<String> gtidFilter) {
        String recordedGtidSetStr = recorded.getString(MySqlOffsetContext.GTID_SET_KEY);
        String desiredGtidSetStr = desired.getString(MySqlOffsetContext.GTID_SET_KEY);
        if (desiredGtidSetStr != null) {
            // The desired position uses GTIDs, so we ideally compare using GTIDs ...
            if (recordedGtidSetStr != null) {
                // Both have GTIDs, so base the comparison entirely on the GTID sets.
                GtidSet recordedGtidSet = new GtidSet(recordedGtidSetStr);
                GtidSet desiredGtidSet = new GtidSet(desiredGtidSetStr);
                if (gtidFilter != null) {
                    // Apply the GTID source filter before we do any comparisons ...
                    recordedGtidSet = recordedGtidSet.retainAll(gtidFilter);
                    desiredGtidSet = desiredGtidSet.retainAll(gtidFilter);
                }
                if (recordedGtidSet.equals(desiredGtidSet)) {
                    // They are exactly the same, which means the recorded position exactly matches the desired ...
                    if (!recorded.has(SNAPSHOT_KEY) && desired.has(SNAPSHOT_KEY)) {
                        // the desired is in snapshot mode, but the recorded is not. So the recorded is *after* the desired ...
                        return false;
                    }
                    // In all other cases (even when recorded is in snapshot mode), recorded is before or at desired GTID.
                    // Now we need to compare how many events in that transaction we've already completed ...
                    int recordedEventCount = recorded.getInteger(MySqlOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int desiredEventCount = desired.getInteger(MySqlOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int diff = recordedEventCount - desiredEventCount;
                    if (diff > 0) {
                        return false;
                    }

                    // Otherwise the recorded is definitely before or at the desired ...
                    return true;
                }
                // The GTIDs are not an exact match, so figure out if recorded is a subset of the desired ...
                return recordedGtidSet.isContainedWithin(desiredGtidSet);
            }
            // The desired position did use GTIDs while the recorded did not use GTIDs. So, we assume that the
            // recorded position is older since GTIDs are often enabled but rarely disabled. And if they are disabled,
            // it is likely that the desired position would not include GTIDs as we would be trying to read the binlog of a
            // server that no longer has GTIDs. And if they are enabled, disabled, and re-enabled, per
            // https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-failover.html all properly configured slaves that
            // use GTIDs should always have the complete set of GTIDs copied from the master, in which case
            // again we know that recorded not having GTIDs is before the desired position ...
            return true;
        }
        else if (recordedGtidSetStr != null) {
            // The recorded has a GTID but the desired does not, so per the previous paragraph we assume that previous
            // is not at or before ...
            return false;
        }

        // Both positions are missing GTIDs. Look at the servers ...
        int recordedServerId = recorded.getInteger(SERVER_ID_KEY, 0);
        int desiredServerId = recorded.getInteger(SERVER_ID_KEY, 0);
        if (recordedServerId != desiredServerId) {
            // These are from different servers, and their binlog coordinates are not related. So the only thing we can do
            // is compare timestamps, and we have to assume that the server timestamps can be compared ...
            long recordedTimestamp = recorded.getLong(TIMESTAMP_KEY, 0);
            long desiredTimestamp = recorded.getLong(TIMESTAMP_KEY, 0);
            return recordedTimestamp <= desiredTimestamp;
        }

        // First compare the MySQL binlog filenames that include the numeric suffix and therefore are lexicographically
        // comparable ...
        String recordedFilename = recorded.getString(BINLOG_FILENAME_OFFSET_KEY);
        String desiredFilename = desired.getString(BINLOG_FILENAME_OFFSET_KEY);
        assert recordedFilename != null;
        int diff = recordedFilename.compareToIgnoreCase(desiredFilename);
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The filenames are the same, so compare the positions ...
        int recordedPosition = recorded.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        int desiredPosition = desired.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        diff = recordedPosition - desiredPosition;
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The positions are the same, so compare the completed events in the transaction ...
        int recordedEventCount = recorded.getInteger(MySqlOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
        int desiredEventCount = desired.getInteger(MySqlOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
        diff = recordedEventCount - desiredEventCount;
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The completed events are the same, so compare the row number ...
        int recordedRow = recorded.getInteger(BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        int desiredRow = desired.getInteger(BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        diff = recordedRow - desiredRow;
        if (diff > 0) {
            return false;
        }

        // The binlog coordinates are the same ...
        return true;
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

    @Deprecated
    public boolean isLastSnapshot() {
        return snapshot() == SnapshotRecord.LAST;
    }
}
