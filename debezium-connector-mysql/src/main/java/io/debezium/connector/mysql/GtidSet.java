/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import io.debezium.annotation.Immutable;

/**
 * A set of MySQL GTIDs. This is an improvement of {@link com.github.shyiko.mysql.binlog.GtidSet} that is immutable,
 * and more properly supports comparisons.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class GtidSet {

    private final String orderedString;
    private final Map<String, UUIDSet> uuidSetsByServerId = new TreeMap<>(); // sorts on keys

    /**
     * @param gtids the string representation of the GTIDs.
     */
    public GtidSet(String gtids) {
        new com.github.shyiko.mysql.binlog.GtidSet(gtids).getUUIDSets().forEach(uuidSet -> {
            uuidSetsByServerId.put(uuidSet.getUUID(), new UUIDSet(uuidSet));
        });
        StringBuilder sb = new StringBuilder();
        uuidSetsByServerId.values().forEach(uuidSet -> {
            if (sb.length() != 0) sb.append(',');
            sb.append(uuidSet.toString());
        });
        orderedString = sb.toString();
    }

    /**
     * Get an immutable collection of the {@link UUIDSet range of GTIDs for a single server}.
     * 
     * @return the {@link UUIDSet GTID ranges for each server}; never null
     */
    public Collection<UUIDSet> getUUIDSets() {
        return Collections.unmodifiableCollection(uuidSetsByServerId.values());
    }

    /**
     * Find the {@link UUIDSet} for the server with the specified UUID.
     * 
     * @param uuid the UUID of the server
     * @return the {@link UUIDSet} for the identified server, or {@code null} if there are no GTIDs from that server.
     */
    public UUIDSet forServerWithId(String uuid) {
        return uuidSetsByServerId.get(uuid);
    }

    /**
     * Determine if the GTIDs represented by this object are contained completely within the supplied set of GTIDs.
     * 
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all of the GTIDs in this set are completely contained within the supplied set of GTIDs, or
     *         {@code false} otherwise
     */
    public boolean isSubsetOf(GtidSet other) {
        if (other == null) return false;
        if (this.equals(other)) return true;
        for (UUIDSet uuidSet : uuidSetsByServerId.values()) {
            UUIDSet thatSet = other.forServerWithId(uuidSet.getUUID());
            if (!uuidSet.isSubsetOf(thatSet)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return orderedString.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.orderedString.equalsIgnoreCase(that.orderedString);
        }
        return false;
    }

    @Override
    public String toString() {
        return orderedString;
    }

    /**
     * A range of GTIDs for a single server with a specific UUID.
     */
    @Immutable
    public static class UUIDSet {

        private String uuid;
        private LinkedList<Interval> intervals = new LinkedList<>();

        protected UUIDSet(com.github.shyiko.mysql.binlog.GtidSet.UUIDSet uuidSet) {
            this.uuid = uuidSet.getUUID();
            uuidSet.getIntervals().forEach(interval -> {
                intervals.add(new Interval(interval.getStart(), interval.getEnd()));
            });
            Collections.sort(this.intervals);
        }

        protected UUIDSet(String uuid, LinkedList<Interval> intervals) {
            this.uuid = uuid;
            this.intervals = intervals;
        }

        /**
         * Get the UUID for the server that generated the GTIDs.
         * 
         * @return the server's UUID; never null
         */
        public String getUUID() {
            return uuid;
        }

        /**
         * Get the intervals of transaction numbers.
         * 
         * @return the immutable transaction intervals; never null
         */
        public Collection<Interval> getIntervals() {
            return Collections.unmodifiableCollection(intervals);
        }

        /**
         * Get the first interval of transaction numbers for this server.
         * 
         * @return the first interval, or {@code null} if there is none
         */
        public Interval getFirstInterval() {
            return intervals.isEmpty() ? null : intervals.getFirst();
        }

        /**
         * Get the last interval of transaction numbers for this server.
         * 
         * @return the last interval, or {@code null} if there is none
         */
        public Interval getLastInterval() {
            return intervals.isEmpty() ? null : intervals.getLast();
        }

        /**
         * Get the interval that contains the full range (and possibly more) of all of the individual intervals for this server.
         * 
         * @return the complete interval comprised of the {@link Interval#getStart() start} of the {@link #getFirstInterval()
         *         first interval} and the {@link Interval#getEnd() end} of the {@link #getLastInterval()}, or {@code null} if
         *         this server has no intervals at all
         */
        public Interval getCompleteInterval() {
            return intervals.isEmpty() ? null : new Interval(getFirstInterval().getStart(), getLastInterval().getEnd());
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction numbers from
         * the set of transaction numbers in the supplied set.
         * 
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are a subset of the transaction numbers of the supplied set,
         *         or false otherwise
         */
        public boolean isSubsetOf(UUIDSet other) {
            if (other == null) return false;
            if (!this.getUUID().equalsIgnoreCase(other.getUUID())) {
                // Not even the same server ...
                return false;
            }
            if (this.intervals.isEmpty()) return true;
            if (other.intervals.isEmpty()) return false;
            assert this.intervals.size() > 0;
            assert other.intervals.size() > 0;

            // Every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isSubsetOf(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) return false; // didn't find a match
            }
            return true;
        }

        @Override
        public int hashCode() {
            return uuid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.getUUID().equalsIgnoreCase(that.getUUID()) && this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (sb.length() != 0) sb.append(',');
            sb.append(uuid).append(':');
            sb.append(intervals.getFirst().getStart());
            sb.append(intervals.getLast().getEnd());
            return sb.toString();
        }
    }

    @Immutable
    public static class Interval extends com.github.shyiko.mysql.binlog.GtidSet.Interval {

        public Interval(long start, long end) {
            super(start, end);
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         * 
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         *         {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied interval's
         *         {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isSubsetOf(Interval other) {
            if (other == this) return true;
            if (other == null) return false;
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj instanceof com.github.shyiko.mysql.binlog.GtidSet.Interval) {
                com.github.shyiko.mysql.binlog.GtidSet.Interval that = (com.github.shyiko.mysql.binlog.GtidSet.Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            return getStart() == getEnd() ? Long.toString(getStart()) : "" + getStart() + "-" + getEnd();
        }
    }
}
