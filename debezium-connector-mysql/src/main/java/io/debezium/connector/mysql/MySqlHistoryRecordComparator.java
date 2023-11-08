/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.function.Predicate;

import io.debezium.document.Document;
import io.debezium.relational.history.HistoryRecordComparator;

final class MySqlHistoryRecordComparator extends HistoryRecordComparator {

    private final Predicate<String> gtidSourceFilter;

    MySqlHistoryRecordComparator(Predicate<String> gtidSourceFilter) {
        super();
        this.gtidSourceFilter = gtidSourceFilter;
    }

    /**
     * Determine whether the first offset is at or before the point in time of the second
     * offset, where the offsets are given in JSON representation of the maps returned by {@link MySqlOffsetContext#getOffset()}.
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
    @Override
    protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
        String recordedGtidSetStr = recorded.getString(MySqlOffsetContext.GTID_SET_KEY);
        String desiredGtidSetStr = desired.getString(MySqlOffsetContext.GTID_SET_KEY);
        if (desiredGtidSetStr != null) {
            // The desired position uses GTIDs, so we ideally compare using GTIDs ...
            if (recordedGtidSetStr != null) {
                // Both have GTIDs, so base the comparison entirely on the GTID sets.
                GtidSet recordedGtidSet = new GtidSet(recordedGtidSetStr);
                GtidSet desiredGtidSet = new GtidSet(desiredGtidSetStr);
                if (gtidSourceFilter != null) {
                    // Apply the GTID source filter before we do any comparisons ...
                    recordedGtidSet = recordedGtidSet.retainAll(gtidSourceFilter);
                    desiredGtidSet = desiredGtidSet.retainAll(gtidSourceFilter);
                }
                if (recordedGtidSet.equals(desiredGtidSet)) {
                    // They are exactly the same, which means the recorded position exactly matches the desired ...
                    if (!recorded.has(SourceInfo.SNAPSHOT_KEY) && desired.has(SourceInfo.SNAPSHOT_KEY)) {
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
            // https://dev.mysql.com/doc/refman/8.2/en/replication-gtids-failover.html all properly configured slaves that
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
        int recordedServerId = recorded.getInteger(SourceInfo.SERVER_ID_KEY, 0);
        int desiredServerId = recorded.getInteger(SourceInfo.SERVER_ID_KEY, 0);
        if (recordedServerId != desiredServerId) {
            // These are from different servers, and their binlog coordinates are not related. So the only thing we can do
            // is compare timestamps, and we have to assume that the server timestamps can be compared ...
            long recordedTimestamp = recorded.getLong(SourceInfo.TIMESTAMP_KEY, 0);
            long desiredTimestamp = recorded.getLong(SourceInfo.TIMESTAMP_KEY, 0);
            return recordedTimestamp <= desiredTimestamp;
        }

        // First compare the MySQL binlog filenames
        BinlogFilename recordedFilename = BinlogFilename.of(recorded.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY));
        BinlogFilename desiredFilename = BinlogFilename.of(desired.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY));
        int diff = recordedFilename.compareTo(desiredFilename);
        if (diff > 0) {
            return false;
        }
        if (diff < 0) {
            return true;
        }

        // The filenames are the same, so compare the positions ...
        int recordedPosition = recorded.getInteger(SourceInfo.BINLOG_POSITION_OFFSET_KEY, -1);
        int desiredPosition = desired.getInteger(SourceInfo.BINLOG_POSITION_OFFSET_KEY, -1);
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
        int recordedRow = recorded.getInteger(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        int desiredRow = desired.getInteger(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
        diff = recordedRow - desiredRow;
        if (diff > 0) {
            return false;
        }

        // The binlog coordinates are the same ...
        return true;
    }

    private static class BinlogFilename implements Comparable<BinlogFilename> {
        final private String baseName;
        final private long extension;

        private BinlogFilename(String baseName, long extension) {
            this.baseName = baseName;
            this.extension = extension;
        }

        public static BinlogFilename of(String filename) {
            int index = filename.lastIndexOf(".");
            if (index == -1) {
                throw new IllegalArgumentException("Filename does not have an extension: " + filename);
            }

            String baseFilename = filename.substring(0, index);
            String stringExtension = filename.substring(index + 1);

            long extension;
            try {
                extension = Long.parseLong(stringExtension);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Can't parse binlog filename extension: " + filename, e);
            }

            return new BinlogFilename(baseFilename, extension);
        }

        @Override
        public String toString() {
            return "BinlogFilename [baseName=" + baseName + ", extension=" + extension + "]";
        }

        @Override
        public int compareTo(BinlogFilename other) {
            if (!baseName.equals(other.baseName)) {
                throw new IllegalArgumentException("Can't compare binlog filenames with different base names");
            }
            return Long.compare(extension, other.extension);
        }
    }
}
