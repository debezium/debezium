/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import java.util.Map;

import io.debezium.annotation.NotThreadSafe;
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
 *         "file" = "mysql-bin.000003",
 *         "pos" = 105586,
 *         "row" = 0
 * }
 * </pre>
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
final class SourceInfo {

    public static final String DATABASE_PARTITION_KEY = "db";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY = "row";

    private String binlogFilename;
    private long binlogPosition = 4;
    private int eventRowNumber = 0;
    private String databaseId;
    private Map<String, ?> sourcePartition;

    public SourceInfo() {
    }

    /**
     * Set the database identifier. This is typically called once upon initialization.
     * 
     * @param logicalId the logical identifier for the database; may not be null
     */
    public void setDatabase(String logicalId) {
        this.databaseId = logicalId;
        sourcePartition = Collect.hashMapOf(DATABASE_PARTITION_KEY, databaseId);
    }

    /**
     * Get the Kafka Connect detail about the source "partition", which describes the portion of the source that we are
     * consuming. Since we're reading the binary log for a single database, the source partition specifies the
     * {@link #setDatabase database server}.
     * <p>
     * The resulting map is mutable for efficiency reasons (this information rarely changes), but should not be mutated.
     * 
     * @return the source partition information; never null
     */
    public Map<String, ?> partition() {
        return sourcePartition;
    }

    /**
     * Get the Kafka Connect detail about the source "offset", which describes the position within the source where we last
     * stopped reading.
     * 
     * @return a copy of the current offset; never null
     */
    public Map<String, Object> offset() {
        return Collect.hashMapOf(BINLOG_FILENAME_OFFSET_KEY, binlogFilename,
                                 BINLOG_POSITION_OFFSET_KEY, binlogPosition,
                                 BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, eventRowNumber);
    }

    /**
     * Set the current row number within a given event, and then get the Kafka Connect detail about the source "offset", which
     * describes the position within the source where we last stopped reading.
     * 
     * @param eventRowNumber the row number within the last event that was successfully processed
     * @return a copy of the current offset; never null
     */
    public Map<String, Object> offset(int eventRowNumber) {
        setRowInEvent(eventRowNumber);
        return offset();
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
     * Set the source offset, as read from Kafka Connect. This method does nothing if the supplied map is null.
     * 
     * @param sourceOffset the previously-recorded Kafka Connect source offset
     */
    public void setOffset(Map<String, Object> sourceOffset) {
        if (sourceOffset != null) {
            // We have previously recorded an offset ...
            binlogFilename = (String) sourceOffset.get(BINLOG_FILENAME_OFFSET_KEY);
            binlogPosition = (Long) sourceOffset.get(BINLOG_POSITION_OFFSET_KEY);
            Integer rowNumber = (Integer) sourceOffset.get(BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY);
            eventRowNumber = rowNumber != null ? rowNumber.intValue() : 0;
        }
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
     * @return the database name; null if it has not been {@link #setDatabase(String) set}
     */
    public String database() {
        return databaseId;
    }
}
