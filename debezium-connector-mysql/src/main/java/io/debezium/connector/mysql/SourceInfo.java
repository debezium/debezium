/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.document.Document;
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
 *     "db" : "myDatabase"
 * }
 * </pre>
 * 
 * <p>
 * The {@link #offset() source offset} information describes how much of the database's binary log the source the change detector
 * has processed. Here's a JSON-like representation of an example:
 * 
 * <pre>
 * {
 *         "gtids" = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
 *         "file" = "mysql-bin.000003",
 *         "pos" = 105586,
 *         "row" = 0
 * }
 * </pre>
 * 
 * Note that the "{@code gtid}" field is present only when GTIDs are enabled.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
final class SourceInfo {

    // Avro Schema doesn't allow "-" to be included as field name, use "_" instead.
    // Ref https://issues.apache.org/jira/browse/AVRO-838.
    public static final String SERVER_ID_KEY = "server_id";

    public static final String SERVER_NAME_KEY = "name";
    public static final String SERVER_PARTITION_KEY = "server";
    public static final String BINLOG_GTID_KEY = "gtids";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY = "row";
    public static final String BINLOG_EVENT_TIMESTAMP_KEY = "ts";
    public static final String BINLOG_SNAPSHOT_KEY = "snapshot";

    /**
     * A {@link Schema} definition for a {@link Struct} used to store the {@link #partition()} and {@link #offset()} information.
     */
    public static final Schema SCHEMA = SchemaBuilder.struct()
                                                     .name("io.debezium.connector.mysql.Source")
                                                     .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                                                     .field(SERVER_ID_KEY, Schema.INT64_SCHEMA)
                                                     .field(BINLOG_EVENT_TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                                                     .field(BINLOG_GTID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                                                     .field(BINLOG_FILENAME_OFFSET_KEY, Schema.STRING_SCHEMA)
                                                     .field(BINLOG_POSITION_OFFSET_KEY, Schema.INT64_SCHEMA)
                                                     .field(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, Schema.INT32_SCHEMA)
                                                     .field(BINLOG_SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                                                     .build();

    private GtidSet binlogGtids;
    private String binlogFilename;
    private long binlogPosition = 4;
    private int eventRowNumber = 0;
    private String serverName;
    private long serverId = 0;
    private long binlogTs = 0;
    private Map<String, String> sourcePartition;
    private boolean snapshot = false;

    public SourceInfo() {
    }

    /**
     * Set the database identifier. This is typically called once upon initialization.
     * 
     * @param logicalId the logical identifier for the database; may not be null
     */
    public void setServerName(String logicalId) {
        this.serverName = logicalId;
        sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
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
     * Get the Kafka Connect detail about the source "offset", which describes the position within the source where we last
     * have last read.
     * 
     * @return a copy of the current offset; never null
     */
    public Map<String, ?> offset() {
        Map<String, Object> map = new HashMap<>();
        if (binlogGtids != null) {
            map.put(BINLOG_GTID_KEY, binlogGtids.toString());
        }
        if (serverId != 0 ) map.put(SERVER_ID_KEY, serverId);
        if (binlogTs != 0 ) map.put(BINLOG_EVENT_TIMESTAMP_KEY, binlogTs);
        map.put(BINLOG_FILENAME_OFFSET_KEY, binlogFilename);
        map.put(BINLOG_POSITION_OFFSET_KEY, binlogPosition);
        map.put(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, eventRowNumber);
        if (isSnapshotInEffect()) {
            map.put(BINLOG_SNAPSHOT_KEY, true);
        }
        return map;
    }

    /**
     * Get a {@link Schema} representation of the source {@link #partition()} and {@link #offset()} information.
     * 
     * @return the source partition and offset {@link Schema}; never null
     * @see #struct()
     */
    public Schema schema() {
        return SCHEMA;
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition()} and {@link #offset()} information. The Struct
     * complies with the {@link #SCHEMA} for the MySQL connector.
     * 
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    public Struct struct() {
        assert serverName != null;
        Struct result = new Struct(SCHEMA);
        result.put(SERVER_NAME_KEY, serverName);
        result.put(SERVER_ID_KEY, serverId);
        if (binlogGtids != null) {
            result.put(BINLOG_GTID_KEY, binlogGtids.toString());
        }
        result.put(BINLOG_FILENAME_OFFSET_KEY, binlogFilename);
        result.put(BINLOG_POSITION_OFFSET_KEY, binlogPosition);
        result.put(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, eventRowNumber);
        result.put(BINLOG_EVENT_TIMESTAMP_KEY, binlogTs);
        if (isSnapshotInEffect()) {
            result.put(BINLOG_SNAPSHOT_KEY, true);
        }
        return result;
    }

    /**
     * Set the current row number within a given event, and then get the Kafka Connect detail about the source "offset", which
     * describes the position within the source where we have last read.
     * 
     * @param eventRowNumber the 0-based row number within the last event that was successfully processed
     * @return a copy of the current offset; never null
     */
    public Map<String, ?> offset(int eventRowNumber) {
        setRowInEvent(eventRowNumber);
        return offset();
    }

    /**
     * Determine whether a snapshot is currently in effect.
     * 
     * @return {@code true} if a snapshot is in effect, or {@code false} otherwise
     */
    public boolean isSnapshotInEffect() {
        return snapshot;
    }

    /**
     * Set the GTID range for the MySQL binary log file.
     * 
     * @param gtids the string representation of the binlog GTIDs; may not be null
     */
    public void setGtids(String gtids) {
        this.binlogGtids = gtids != null && !gtids.trim().isEmpty() ? new GtidSet(gtids) : null;
    }

    /**
     * Set the name of the MySQL binary log file.
     * 
     * @param binlogFilename the name of the binary log file; may not be null
     */
    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    /**
     * Set the position within the MySQL binary log file.
     * 
     * @param binlogPosition the position within the binary log file
     */
    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    /**
     * Set the index of the row within the event appearing at the {@link #binlogPosition() position} within the
     * {@link #binlogFilename() binary log file}.
     * 
     * @param rowNumber the 0-based row number
     */
    public void setRowInEvent(int rowNumber) {
        this.eventRowNumber = rowNumber;
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
     * Set the timestamp as found within the MySQL binary log file.
     * 
     * @param timestamp the timestamp found within the binary log file
     */
    public void setBinlogTimestamp(long timestamp) {
        this.binlogTs = timestamp;
    }

    /**
     * Denote that a snapshot is being (or has been) started.
     */
    public void startSnapshot() {
        this.snapshot = true;
    }

    /**
     * Denote that a snapshot is being (or has been) started.
     */
    public void completeSnapshot() {
        this.snapshot = false;
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
            setGtids((String) sourceOffset.get(BINLOG_GTID_KEY)); // may be null
            binlogFilename = (String) sourceOffset.get(BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + BINLOG_FILENAME_OFFSET_KEY + "' parameter is missing");
            }
            binlogPosition = longOffsetValue(sourceOffset, BINLOG_POSITION_OFFSET_KEY);
            eventRowNumber = (int) longOffsetValue(sourceOffset, BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY);
        }
    }

    private long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).longValue();
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' parameter value " + obj + " could not be converted to a long");
        }
    }

    /**
     * Get the string representation of the GTID range for the MySQL binary log file.
     * 
     * @return the string representation of the binlog GTID ranges; may be null
     */
    public String gtidSet() {
        return this.binlogGtids != null ? this.binlogGtids.toString() : null;
    }

    /**
     * Get the name of the MySQL binary log file that has been processed.
     * 
     * @return the name of the binary log file; null if it has not been {@link #setBinlogFilename(String) set}
     */
    public String binlogFilename() {
        return binlogFilename;
    }

    /**
     * Get the position within the MySQL binary log file that has been processed.
     * 
     * @return the position within the binary log file; null if it has not been {@link #setBinlogPosition(long) set}
     */
    public long binlogPosition() {
        return binlogPosition;
    }

    /**
     * Get the row within the event at the {@link #binlogPosition() position} within the {@link #binlogFilename() binary log file}
     * .
     * 
     * @return the 0-based row number
     */
    public int eventRowNumber() {
        return eventRowNumber;
    }

    /**
     * Get the logical identifier of the database that is the source of the events.
     * 
     * @return the database name; null if it has not been {@link #setServerName(String) set}
     */
    public String serverName() {
        return serverName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (binlogGtids != null) {
            sb.append("GTIDs ");
            sb.append(binlogGtids);
            sb.append(" and binlog file '").append(binlogFilename).append("'");
            sb.append(", pos=").append(binlogPosition());
            sb.append(", row=").append(eventRowNumber());
        } else {
            if (binlogFilename == null) {
                sb.append("<latest>");
            } else {
                if ("".equals(binlogFilename)) {
                    sb.append("earliest binlog file and position");
                } else {
                    sb.append("binlog file '").append(binlogFilename).append("'");
                    sb.append(", pos=").append(binlogPosition());
                    sb.append(", row=").append(eventRowNumber());
                }
            }
        }
        return sb.toString();
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
     * @return {@code true} if the recorded position is at or before the desired position; or {@code false} otherwise
     */
    public static boolean isPositionAtOrBefore(Document recorded, Document desired) {
        String recordedGtidSetStr = recorded.getString(BINLOG_GTID_KEY);
        String desiredGtidSetStr = desired.getString(BINLOG_GTID_KEY);
        if (desiredGtidSetStr != null) {
            // The desired position uses GTIDs, so we ideally compare using GTIDs ...
            if (recordedGtidSetStr != null) {
                // Both have GTIDs, so base the comparison entirely on the GTID sets.
                GtidSet recordedGtidSet = new GtidSet(recordedGtidSetStr);
                GtidSet desiredGtidSet = new GtidSet(desiredGtidSetStr);
                if ( recordedGtidSet.equals(desiredGtidSet)) {
                    // They are exactly the same, which means the recorded position exactly matches the desired ...
                    if ( !recorded.has(BINLOG_SNAPSHOT_KEY) && desired.has(BINLOG_SNAPSHOT_KEY)) {
                        // the desired is in snapshot mode, but the recorded is not. So the recorded is *after* the desired ...
                        return false;
                    }
                    // In all other cases (even when recorded is in snapshot mode), recorded is before or at desired ...
                    return true;
                }
                // The GTIDs are not an exact match, so figure out if recorded is a subset of the desired ...
                return recordedGtidSet.isSubsetOf(desiredGtidSet);
            }
            // The desired position did use GTIDs while the recorded did not use GTIDs. So, we assume that the
            // recorded position is older since GTIDs are often enabled but rarely disabled. And if they are disabled,
            // it is likely that the desired position would not include GTIDs as we would be trying to read the binlog of a
            // server that no longer has GTIDs. And if they are enabled, disabled, and re-enabled, per
            // https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-failover.html all properly configured slaves that
            // use GTIDs should always have the complete set of GTIDs copied from the master, in which case
            // again we know that recorded not having GTIDs is before the desired position ...
            return true;
        } else if (recordedGtidSetStr != null) {
            // The recorded has a GTID but the desired does not, so per the previous paragraph we assume that previous
            // is not at or before ...
            return false;
        }

        // Both positions are missing GTIDs. Look at the servers ...
        int recordedServerId = recorded.getInteger(SERVER_ID_KEY,0);
        int desiredServerId = recorded.getInteger(SERVER_ID_KEY,0);
        if ( recordedServerId != desiredServerId ) {
            // These are from different servers, and their binlog coordinates are not related. So the only thing we can do
            // is compare timestamps, and we have to assume that the server timestamps can be compared ...
            long recordedTimestamp = recorded.getLong(BINLOG_EVENT_TIMESTAMP_KEY,0);
            long desiredTimestamp = recorded.getLong(BINLOG_EVENT_TIMESTAMP_KEY,0);
            return recordedTimestamp <= desiredTimestamp;
        }
        
        // First compare the MySQL binlog filenames that include the numeric suffix and therefore are lexicographically
        // comparable ...
        String recordedFilename = recorded.getString(BINLOG_FILENAME_OFFSET_KEY);
        String desiredFilename = desired.getString(BINLOG_FILENAME_OFFSET_KEY);
        assert recordedFilename != null;
        int diff = recordedFilename.compareToIgnoreCase(desiredFilename);
        if ( diff > 0 ) return false;

        // The filenames are the same, so compare the positions ...
        int recordedPosition = recorded.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        int desiredPosition = desired.getInteger(BINLOG_POSITION_OFFSET_KEY, -1);
        diff = recordedPosition - desiredPosition;
        if ( diff > 0 ) return false;
        
        // The positions are the same, so compare the row number ...
        int recordedRow = recorded.getInteger(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, -1);
        int desiredRow = desired.getInteger(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, -1);
        diff = recordedRow - desiredRow;
        if ( diff > 0 ) return false;

        // The binlog coordinates are the same ...
        return true;
    }
}
