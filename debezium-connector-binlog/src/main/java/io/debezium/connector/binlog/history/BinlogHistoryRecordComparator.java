/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.history;

import java.util.function.Predicate;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.document.Document;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * Base implementation of the {@link HistoryRecordComparator} for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogHistoryRecordComparator extends HistoryRecordComparator {

    private final Predicate<String> gtidSourceFilter;
    private final GtidSetFactory gtidSetFactory;

    public BinlogHistoryRecordComparator(Predicate<String> gtidSourceFilter, GtidSetFactory gtidSetFactory) {
        this.gtidSourceFilter = gtidSourceFilter;
        this.gtidSetFactory = gtidSetFactory;
    }

    /**
     * Determine whether the first offset is at or before the point in time of the second offset,
     * where the offsets are given in JSON representations of maps returned by the connector's
     * offset context.<p></p>
     *
     * This logic makes a significant assumption: once a server enables GTID, they are never disabled.
     * This is the only way to compare a position with a GTID to a position without a GTID, and any
     * change with a GTID is <em>after</em> positions without.<p></p>
     *
     * When both positions have GTIDs, the positions are compared using the GTIDs. If the GTID values
     * are identical, then we compare whether they have snapshots enabled.
     *
     * @param recorded the position obtained from the recorded history; never null
     * @param desired the desired position that we want to obtain, which should be after some recorded
     *                positions at some recorded positions, and before other recorded positions; never null
     * @return true if the recorded position is at or before the desired position; false otherwise.
     */
    @Override
    @VisibleForTesting
    public boolean isPositionAtOrBefore(Document recorded, Document desired) {
        final String recordedGtid = getGtidSet(recorded);
        final String desiredGtid = getGtidSet(desired);
        if (desiredGtid != null) {
            // The desired position uses GTID
            if (recordedGtid != null) {
                // Both positions have GTID, use GTID comparison
                GtidSet recordedGtidSet = gtidSetFactory.createGtidSet(recordedGtid);
                GtidSet desiredGtidSet = gtidSetFactory.createGtidSet(desiredGtid);
                if (gtidSourceFilter != null) {
                    // Apply GTID source filter
                    recordedGtidSet = recordedGtidSet.retainAll(gtidSourceFilter);
                    desiredGtidSet = desiredGtidSet.retainAll(gtidSourceFilter);
                }
                if (recordedGtidSet.equals(desiredGtidSet)) {
                    // These are exactly the same, recorded position and desired positions match
                    if (!isSnapshot(recorded) && isSnapshot(desired)) {
                        // The desired is in snapshot mode, but the recorded is not, so its *after* the desired
                        return false;
                    }
                    // In all other cases, recorded is before or at desired GTID
                    // Now compare the number of events in the transaction
                    int recordedEventCount = recorded.getInteger(BinlogOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int desiredEventCount = desired.getInteger(BinlogOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
                    int diff = recordedEventCount - desiredEventCount;
                    if (diff > 0) {
                        return false;
                    }
                    // Otherwise recorded is before the desired
                    return true;
                }
                // Not exact match, determine if recorded is subset of desired
                return recordedGtidSet.isContainedWithin(desiredGtidSet);
            }
            // The desired position did use GTID while the recorded did not.
            // Assume that the recorded position is older since GTIDs are often enabled but rarely disabled.
            // If they are disabled, it is likely that the desired position would not include GTIDs as we
            // would be reading a binlog of a server that no longer has GTIDs. If they are enabled, disabled,
            // and then re-enabled, per https://dev.mysql.com/doc/refman/8.2/en/replication-gtids-failover.html,
            // all properly configured replicas that use GTIDs should always have the complete set of GTIDs
            // copied from the primary, in which case we know that recorded not having GTID is before desired.
            return true;
        }
        else if (recordedGtid != null) {
            // The recorded has a GTID but the desired does not.
            // We assume that previous is not at or before based on previous paragraph.
            return false;
        }

        // Both positions are missing GTIDs, compare servers
        if (getServerId(recorded) != getServerId(desired)) {
            // These are from different servers.
            // Their binlog coordinates are not related, so the only thing that is possible is to compare
            // timestamps, and assume that the server timestamps can be compared.
            return getTimestamp(recorded) <= getTimestamp(desired);
        }

        // Compare binlog file names
        final BinlogFileName recordedFileName = getBinlogFileName(recorded);
        final BinlogFileName desiredFileName = getBinlogFileName(desired);
        final int fileNameCheck = recordedFileName.compareTo(desiredFileName);
        if (fileNameCheck != 0) {
            return fileNameCheck < 0;
        }

        // With the filenames the same, compare positions
        final int recordedPosition = getBinlogPosition(recorded);
        final int desiredPosition = getBinlogPosition(desired);
        final int positionCheck = recordedPosition - desiredPosition;
        if (positionCheck != 0) {
            return positionCheck < 0;
        }

        // The positions are the same, so compare the completed events in the transaction ...
        final int recordedEventCount = getEventsToSkip(recorded);
        final int desiredEventCount = getEventsToSkip(desired);
        final int eventCountCheck = recordedEventCount - desiredEventCount;
        if (eventCountCheck != 0) {
            return eventCountCheck < 0;
        }

        // The completed events are the same, so compare the row number ...
        final int recordedRow = getBinlogRowInEvent(recorded);
        final int desiredRow = getBinlogRowInEvent(desired);
        final int rowCheck = recordedRow - desiredRow;
        return rowCheck <= 0;
    }

    /**
     * Get the global transaction identifier set.
     *
     * @param document the document to inspect, should not be null
     * @return the global transaction identifier set as a string
     */
    protected String getGtidSet(Document document) {
        return document.getString(BinlogOffsetContext.GTID_SET_KEY);
    }

    /**
     * Get the server unique identifier.
     *
     * @param document the document to inspect, should not be null
     * @return the unique server identifier
     */
    protected int getServerId(Document document) {
        return document.getInteger(BinlogSourceInfo.SERVER_ID_KEY, 0);
    }

    /**
     * Get whether the event is part of the connector's snapshot phase.
     *
     * @param document the document to inspect, should not be null
     * @return true if its part of the snapshot, false otherwise
     */
    protected boolean isSnapshot(Document document) {
        return document.has(BinlogSourceInfo.SNAPSHOT_KEY);
    }

    /**
     * Get the timestamp.
     *
     * @param document the document to inspect, should not be null
     * @return the timestamp value
     */
    protected long getTimestamp(Document document) {
        return document.getLong(BinlogSourceInfo.TIMESTAMP_KEY, 0);
    }

    /**
     * Get the binlog file name.
     *
     * @param document the document to inspect, should not be null
     * @return the binlog file name value
     */
    protected BinlogFileName getBinlogFileName(Document document) {
        return BinlogFileName.of(document.getString(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY));
    }

    /**
     * Get the binlog position.
     *
     * @param document the document to inspect, should not be null
     * @return the binlog position value
     */
    protected int getBinlogPosition(Document document) {
        return document.getInteger(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY, -1);
    }

    /**
     * Get the number of events to skip.
     *
     * @param document the document to inspect, should not be null
     * @return the binlog number of events to skip value
     */
    protected int getEventsToSkip(Document document) {
        return document.getInteger(BinlogOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
    }

    /**
     * Get the binlog row in event value.
     *
     * @param document the document to inspect, should not be null
     * @return the binlog row in event value
     */
    protected int getBinlogRowInEvent(Document document) {
        return document.getInteger(BinlogSourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
    }

    protected static class BinlogFileName implements Comparable<BinlogFileName> {
        private final String baseName;
        private final long extension;

        private BinlogFileName(String baseName, long extension) {
            this.baseName = baseName;
            this.extension = extension;
        }

        @Override
        public int compareTo(BinlogFileName other) {
            if (!baseName.equals(other.baseName)) {
                throw new IllegalArgumentException("Cannot compare binlog filenames with different base names");
            }
            return Long.compare(extension, other.extension);
        }

        @Override
        public String toString() {
            return "BinlogFileName [baseName=" + baseName + ", extension=" + extension + "]";
        }

        /**
         * Constructs a {@link }BinlogFileName} from a filename string.
         *
         * @param fileName the filename to be parsed
         * @return a binlog filename instance
         * @throws IllegalArgumentException if there is a problem parsing the provided file name
         */
        public static BinlogFileName of(String fileName) {
            int index = fileName.lastIndexOf('.');
            if (index == -1) {
                throw new IllegalArgumentException("Filename does not have an extension:" + fileName);
            }

            final String baseFileName = fileName.substring(0, index);
            final String stringExtension = fileName.substring(index + 1);

            long extension;
            try {
                extension = Long.parseLong(stringExtension);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot parse binlog filename extension: " + fileName, e);
            }

            return new BinlogFileName(baseFileName, extension);
        }
    }
}
