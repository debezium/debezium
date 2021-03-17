/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * Information about the source of information, which includes the position in the source binary log we have previously processed.
 * <p>
 * The {@link #partition() source partition} information describes the database whose log is being consumed. Typically, the
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
 * The {@link #offset() source offset} information is included in each event and captures where the connector should restart
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
 * particular event, including a mixture the fields from the {@link #partition() partition} (which is renamed in the source to
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

    public static final String SERVER_PARTITION_KEY = "server";
    public static final String GTID_SET_KEY = "gtids";
    public static final String GTID_KEY = "gtid";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String THREAD_KEY = "thread";
    public static final String QUERY_KEY = "query";
    public static final String DATABASE_WHITELIST_KEY = "database_whitelist";
    public static final String DATABASE_INCLUDE_LIST_KEY = "database_include_list";
    public static final String DATABASE_BLACKLIST_KEY = "database_blacklist";
    public static final String DATABASE_EXCLUDE_LIST_KEY = "database_exclude_list";
    public static final String TABLE_WHITELIST_KEY = "table_whitelist";
    public static final String TABLE_INCLUDE_LIST_KEY = "table_include_list";
    public static final String TABLE_BLACKLIST_KEY = "table_blacklist";
    public static final String TABLE_EXCLUDE_LIST_KEY = "table_exclude_list";
    public static final String RESTART_PREFIX = "RESTART_";

    private String currentGtidSet;
    private String currentGtid;
    private String currentBinlogFilename;
    private long currentBinlogPosition = 0L;
    private int currentRowNumber = 0;
    private long currentEventLengthInBytes = 0;
    private String restartGtidSet;
    private String restartBinlogFilename;
    private long restartBinlogPosition = 0L;
    private long restartEventsToSkip = 0;
    private int restartRowsToSkip = 0;
    private boolean inTransaction = false;
    private long serverId = 0;
    private Instant sourceTime = null;
    private long threadId = -1L;
    private final Map<String, String> sourcePartition;
    private boolean lastSnapshot = true;
    private boolean nextSnapshot = false;
    private String currentQuery = null;
    private String databaseIncludeList;
    private String databaseExcludeList;
    private String tableIncludeList;
    private String tableExcludeList;
    private Set<TableId> tableIds;
    private String databaseName;

    public SourceInfo(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);

        this.sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
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
     * Get the Kafka Connect detail about the source "partition", which describes the portion of the source that we are
     * consuming. Since we're reading the binary log for a single database, the source partition specifies the
     * {@link #setServerName(String) database server}.
     * <p>
     * The resulting map is mutable for efficiency reasons (this information rarely changes), but should not be mutated.
     *
     * @return the source partition information; never null
     */
    public Map<String, String> partition() {
        return sourcePartition;
    }

    /**
     * Set the position in the MySQL binlog where we will start reading.
     *
     * @param binlogFilename the name of the binary log file; may not be null
     * @param positionOfFirstEvent the position in the binary log file to begin processing
     */
    public void setBinlogStartPoint(String binlogFilename, long positionOfFirstEvent) {
        if (binlogFilename != null) {
            this.currentBinlogFilename = binlogFilename;
            this.restartBinlogFilename = binlogFilename;
        }
        assert positionOfFirstEvent >= 0;
        this.currentBinlogPosition = positionOfFirstEvent;
        this.restartBinlogPosition = positionOfFirstEvent;
        this.currentRowNumber = 0;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
    }

    /**
     * Set the position within the MySQL binary log file of the <em>current event</em>.
     *
     * @param positionOfCurrentEvent the position within the binary log file of the current event
     * @param eventSizeInBytes the size in bytes of this event
     */
    public void setEventPosition(long positionOfCurrentEvent, long eventSizeInBytes) {
        this.currentBinlogPosition = positionOfCurrentEvent;
        this.currentEventLengthInBytes = eventSizeInBytes;
        if (!inTransaction) {
            this.restartBinlogPosition = positionOfCurrentEvent + eventSizeInBytes;
            this.restartRowsToSkip = 0;
            this.restartEventsToSkip = 0;
        }
        // Don't set anything else, since the row numbers are set in the offset(int,int) method called at least once
        // for each processed event
    }

    /**
     * Get the Kafka Connect detail about the source "offset", which describes the position within the source where we last
     * have last read.
     *
     * @return a copy of the current offset; never null
     */
    public Map<String, ?> offset() {
        return offsetUsingPosition(this.restartRowsToSkip);
    }

    /**
     * Given the row number within a binlog event and the total number of rows in that event, compute and return the
     * Kafka Connect offset that is be included in the produced change event describing the row.
     * <p>
     * This method should always be called before {@link #struct()}.
     *
     * @param eventRowNumber the 0-based row number within the event for which the offset is to be produced
     * @param totalNumberOfRows the total number of rows within the event being processed
     * @return a copy of the current offset; never null
     * @see #struct()
     */
    public Map<String, Object> offsetForRow(int eventRowNumber, int totalNumberOfRows) {
        if (eventRowNumber < (totalNumberOfRows - 1)) {
            // This is not the last row, so our offset should record the next row to be used ...
            this.currentRowNumber = eventRowNumber;
            this.restartRowsToSkip = this.currentRowNumber + 1;
            // so write out the offset with the position of this event
            return offsetUsingPosition(this.restartRowsToSkip);
        }
        // This is the last row, so write out the offset that has the position of the next event ...
        this.currentRowNumber = eventRowNumber;
        this.restartRowsToSkip = 0;
        return offsetUsingPosition(totalNumberOfRows);
    }

    public void changeEventCompleted() {
        this.restartRowsToSkip = 0;
    }

    private Map<String, Object> offsetUsingPosition(long rowsToSkip) {
        Map<String, Object> map = new HashMap<>();
        if (serverId != 0) {
            map.put(SERVER_ID_KEY, serverId);
        }
        if (restartGtidSet != null) {
            // Put the previously-completed GTID set in the offset along with the event number ...
            map.put(GTID_SET_KEY, restartGtidSet);
        }
        map.put(BINLOG_FILENAME_OFFSET_KEY, restartBinlogFilename);
        map.put(BINLOG_POSITION_OFFSET_KEY, restartBinlogPosition);
        if (restartEventsToSkip != 0) {
            map.put(EVENTS_TO_SKIP_OFFSET_KEY, restartEventsToSkip);
        }
        if (rowsToSkip != 0) {
            map.put(BINLOG_ROW_IN_EVENT_OFFSET_KEY, rowsToSkip);
        }
        if (sourceTime != null) {
            map.put(TIMESTAMP_KEY, sourceTime.getEpochSecond());
        }
        if (isSnapshotInEffect()) {
            map.put(SNAPSHOT_KEY, true);
        }
        if (hasFilterInfo()) {
            map.put(DATABASE_INCLUDE_LIST_KEY, databaseIncludeList);
            map.put(DATABASE_EXCLUDE_LIST_KEY, databaseExcludeList);
            map.put(TABLE_INCLUDE_LIST_KEY, tableIncludeList);
            map.put(TABLE_EXCLUDE_LIST_KEY, tableExcludeList);
        }
        return map;
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

    /**
     * Determine whether a snapshot is currently in effect.
     *
     * @return {@code true} if a snapshot is in effect, or {@code false} otherwise
     */
    public boolean isSnapshotInEffect() {
        return nextSnapshot;
    }

    public void startNextTransaction() {
        // If we have to restart, then we'll start with this BEGIN transaction
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.restartBinlogFilename = this.currentBinlogFilename;
        this.restartBinlogPosition = this.currentBinlogPosition;
        this.inTransaction = true;
    }

    /**
     * Capture that we're starting a new event.
     */
    public void completeEvent() {
        ++restartEventsToSkip;
    }

    /**
     * Get the number of events after the last transaction BEGIN that we've already processed.
     *
     * @return the number of events in the transaction that have been processed completely
     * @see #completeEvent()
     * @see #startNextTransaction()
     */
    public long eventsToSkipUponRestart() {
        return restartEventsToSkip;
    }

    public void commitTransaction() {
        this.restartGtidSet = this.currentGtidSet;
        this.restartBinlogFilename = this.currentBinlogFilename;
        this.restartBinlogPosition = this.currentBinlogPosition + this.currentEventLengthInBytes;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.inTransaction = false;
        this.currentQuery = null;
    }

    /**
     * Record that a new GTID transaction has been started and has been included in the set of GTIDs known to the MySQL server.
     *
     * @param gtid the string representation of a specific GTID that has been begun; may not be null
     * @param gtidSet the string representation of GTID set that includes the newly begun GTID; may not be null
     */
    public void startGtid(String gtid, String gtidSet) {
        this.currentGtid = gtid;
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replaceAll("\n", "").replaceAll("\r", "");
            // Set the GTID set that we'll use if restarting BEFORE successful completion of the events in this GTID ...
            this.restartGtidSet = this.currentGtidSet != null ? this.currentGtidSet : trimmedGtidSet;
            // Record the GTID set that includes the current transaction ...
            this.currentGtidSet = trimmedGtidSet;
        }
    }

    /**
     * Set the GTID set that captures all of the GTID transactions that have been completely processed.
     *
     * @param gtidSet the string representation of the GTID set; may not be null, but may be an empty string if no GTIDs
     *            have been previously processed
     */
    public void setCompletedGtidSet(String gtidSet) {
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replaceAll("\n", "").replaceAll("\r", "");
            this.currentGtidSet = trimmedGtidSet;
            this.restartGtidSet = trimmedGtidSet;
        }
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
     * Denote that a snapshot is being (or has been) started.
     */
    public void startSnapshot() {
        this.lastSnapshot = true;
        this.nextSnapshot = true;
    }

    /**
     * Denote that a snapshot will be complete after one last record.
     */
    public void markLastSnapshot(Configuration config) {
        this.lastSnapshot = true;
        this.nextSnapshot = false;
        maybeSetFilterDataFromConfig(config);
    }

    /**
     * Denote that a snapshot has completed successfully.
     */
    public void completeSnapshot() {
        this.lastSnapshot = false;
        this.nextSnapshot = false;
    }

    /**
     * Set the filter data for the offset from the given {@link Configuration}
     * @param config the configuration
     */
    public void setFilterDataFromConfig(Configuration config) {
        this.databaseIncludeList = config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, MySqlConnectorConfig.DATABASE_WHITELIST);
        this.databaseExcludeList = config.getFallbackStringProperty(MySqlConnectorConfig.DATABASE_EXCLUDE_LIST, MySqlConnectorConfig.DATABASE_BLACKLIST);
        this.tableIncludeList = config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_INCLUDE_LIST, MySqlConnectorConfig.TABLE_WHITELIST);
        this.tableExcludeList = config.getFallbackStringProperty(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, MySqlConnectorConfig.TABLE_BLACKLIST);
    }

    /**
     * Set filter data from config if and only if parallel snapshotting of new tables is turned on
     * @param config the configuration.
     */
    public void maybeSetFilterDataFromConfig(Configuration config) {
        if (config.getString(MySqlConnectorConfig.SNAPSHOT_NEW_TABLES).equals(
                MySqlConnectorConfig.SnapshotNewTables.PARALLEL.getValue())) {
            setFilterDataFromConfig(config);
        }
        else {
            if (hasFilterInfo()) {
                // Connector has filter info but snapshot.new.tables is disabled. Filter info should be unset.
                this.databaseIncludeList = null;
                this.databaseExcludeList = null;
                this.tableIncludeList = null;
                this.tableExcludeList = null;
            }
        }
    }

    /**
     * @return true if this offset has filter info, false otherwise.
     */
    public boolean hasFilterInfo() {
        /*
         * There are 2 possible cases for us not having filter info.
         * 1. The connector does not use a filter. Creating a filter in such a connector could never add any tables.
         * 2. The initial snapshot occurred in a version of Debezium that did not store the filter information in the
         * offsets / the connector was not configured to store filter information.
         */
        return databaseIncludeList != null || databaseExcludeList != null ||
                tableIncludeList != null || tableExcludeList != null;
    }

    public String getDatabaseIncludeList() {
        return databaseIncludeList;
    }

    public String getDatabaseExcludeList() {
        return databaseExcludeList;
    }

    public String getTableIncludeList() {
        return tableIncludeList;
    }

    public String getTableExcludeList() {
        return tableExcludeList;
    }

    /**
     * Set the source offset, as read from Kafka Connect. This method does nothing if the supplied map is null.
     *
     * @param sourceOffset the previously-recorded Kafka Connect source offset
     * @throws ConnectException if any offset parameter values are missing, invalid, or of the wrong type
     */
    public void setOffset(Map<String, ?> sourceOffset) {
        if (sourceOffset != null) {
            // We have previously recorded an offset ...
            setCompletedGtidSet((String) sourceOffset.get(GTID_SET_KEY)); // may be null
            String binlogFilename = (String) sourceOffset.get(BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + BINLOG_FILENAME_OFFSET_KEY + "' parameter is missing");
            }
            long binlogPosition = longOffsetValue(sourceOffset, BINLOG_POSITION_OFFSET_KEY);
            setBinlogStartPoint(binlogFilename, binlogPosition);
            this.restartRowsToSkip = (int) longOffsetValue(sourceOffset, BINLOG_ROW_IN_EVENT_OFFSET_KEY);
            this.restartEventsToSkip = longOffsetValue(sourceOffset, EVENTS_TO_SKIP_OFFSET_KEY);
            nextSnapshot = booleanOffsetValue(sourceOffset, SNAPSHOT_KEY);
            lastSnapshot = nextSnapshot;
            if (sourceOffset.containsKey(DATABASE_INCLUDE_LIST_KEY)) {
                this.databaseIncludeList = (String) sourceOffset.get(DATABASE_INCLUDE_LIST_KEY);
            }
            else {
                this.databaseIncludeList = (String) sourceOffset.get(DATABASE_WHITELIST_KEY);
            }
            if (sourceOffset.containsKey(DATABASE_EXCLUDE_LIST_KEY)) {
                this.databaseExcludeList = (String) sourceOffset.get(DATABASE_EXCLUDE_LIST_KEY);
            }
            else {
                this.databaseExcludeList = (String) sourceOffset.get(DATABASE_BLACKLIST_KEY);
            }
            if (sourceOffset.containsKey(TABLE_INCLUDE_LIST_KEY)) {
                this.tableIncludeList = (String) sourceOffset.get(TABLE_INCLUDE_LIST_KEY);
            }
            else {
                this.tableIncludeList = (String) sourceOffset.get(TABLE_WHITELIST_KEY);
            }
            if (sourceOffset.containsKey(TABLE_EXCLUDE_LIST_KEY)) {
                this.tableExcludeList = (String) sourceOffset.get(TABLE_EXCLUDE_LIST_KEY);
            }
            else {
                this.tableExcludeList = (String) sourceOffset.get(TABLE_BLACKLIST_KEY);
            }
        }
    }

    public static boolean offsetsHaveFilterInfo(Map<String, ?> sourceOffset) {
        return sourceOffset != null && (sourceOffset.containsKey(DATABASE_BLACKLIST_KEY) ||
                sourceOffset.containsKey(DATABASE_EXCLUDE_LIST_KEY) ||
                sourceOffset.containsKey(DATABASE_WHITELIST_KEY) ||
                sourceOffset.containsKey(DATABASE_INCLUDE_LIST_KEY) ||
                sourceOffset.containsKey(TABLE_BLACKLIST_KEY) ||
                sourceOffset.containsKey(TABLE_EXCLUDE_LIST_KEY) ||
                sourceOffset.containsKey(TABLE_WHITELIST_KEY) ||
                sourceOffset.containsKey(TABLE_INCLUDE_LIST_KEY));
    }

    private long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0L;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        }
        catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' parameter value " + obj + " could not be converted to a long");
        }
    }

    private boolean booleanOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return false;
        }
        if (obj instanceof Boolean) {
            return ((Boolean) obj).booleanValue();
        }
        return Boolean.parseBoolean(obj.toString());
    }

    /**
     * Get the string representation of the GTID range for the MySQL binary log file.
     *
     * @return the string representation of the binlog GTID ranges; may be null
     */
    public String gtidSet() {
        return this.currentGtidSet != null ? this.currentGtidSet : null;
    }

    /**
     * Get the name of the MySQL binary log file that has last been processed.
     *
     * @return the name of the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    public String binlogFilename() {
        return restartBinlogFilename;
    }

    /**
     * Get the position within the MySQL binary log file of the next event to be processed.
     *
     * @return the position within the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    public long binlogPosition() {
        return restartBinlogPosition;
    }

    /**
     * Get the position within the MySQL binary log file of the most recently processed event.
     *
     * @return the position within the binary log file; null if it has not been {@link #setBinlogStartPoint(String, long) set}
     */
    protected long restartBinlogPosition() {
        return restartBinlogPosition;
    }

    /**
     * Get the number of rows beyond the {@link #eventsToSkipUponRestart() last completely processed event} to be skipped
     * upon restart.
     *
     * @return the number of rows to be skipped
     */
    public int rowsToSkipUponRestart() {
        return restartRowsToSkip;
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

    boolean isLastSnapshot() {
        return lastSnapshot;
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
        StringBuilder sb = new StringBuilder();
        if (currentGtidSet != null) {
            sb.append("GTIDs ");
            sb.append(currentGtidSet);
            sb.append(" and binlog file '").append(restartBinlogFilename).append("'");
            sb.append(", pos=").append(restartBinlogPosition);
            sb.append(", skipping ").append(restartEventsToSkip);
            sb.append(" events plus ").append(restartRowsToSkip);
            sb.append(" rows");
        }
        else {
            if (restartBinlogFilename == null) {
                sb.append("<latest>");
            }
            else {
                if ("".equals(restartBinlogFilename)) {
                    sb.append("earliest binlog file and position");
                }
                else {
                    sb.append("binlog file '").append(restartBinlogFilename).append("'");
                    sb.append(", pos=").append(restartBinlogPosition);
                    sb.append(", skipping ").append(restartEventsToSkip);
                    sb.append(" events plus ").append(restartRowsToSkip);
                    sb.append(" rows");
                }
            }
        }
        return sb.toString();
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
     */
    public static boolean isPositionAtOrBefore(Document recorded, Document desired, Predicate<String> gtidFilter) {
        String recordedGtidSetStr = recorded.getString(GTID_SET_KEY);
        String desiredGtidSetStr = desired.getString(GTID_SET_KEY);
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
                    int recordedEventCount = recorded.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int desiredEventCount = desired.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
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
        int recordedEventCount = recorded.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
        int desiredEventCount = desired.getInteger(EVENTS_TO_SKIP_OFFSET_KEY, 0);
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
    protected SnapshotRecord snapshot() {
        if (isSnapshotInEffect()) {
            return SnapshotRecord.TRUE;
        }
        return SnapshotRecord.FALSE;
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
