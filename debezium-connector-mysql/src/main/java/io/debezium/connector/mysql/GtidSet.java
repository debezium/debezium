/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;

/**
 * A set of MySQL GTIDs. This is an improvement of {@link com.github.shyiko.mysql.binlog.GtidSet} that is immutable,
 * and more properly supports comparisons.
 *
 * @author Randall Hauch
 */
@Immutable
public final class GtidSet {

    private final Map<String, UUIDSet> uuidSetsByServerId = new TreeMap<>(); // sorts on keys
    public static Pattern GTID_DELIMITER = Pattern.compile(":");

    protected GtidSet(Map<String, UUIDSet> uuidSetsByServerId) {
        this.uuidSetsByServerId.putAll(uuidSetsByServerId);
    }

    /**
     * @param gtids the string representation of the GTIDs.
     */
    public GtidSet(String gtids) {
        gtids = gtids.replace("\n", "").replace("\r", "");
        new com.github.shyiko.mysql.binlog.GtidSet(gtids).getUUIDSets().forEach(uuidSet -> {
            uuidSetsByServerId.put(uuidSet.getUUID(), new UUIDSet(uuidSet));
        });
        StringBuilder sb = new StringBuilder();
        uuidSetsByServerId.values().forEach(uuidSet -> {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(uuidSet.toString());
        });
    }

    /**
     * Obtain a copy of this {@link GtidSet} except with only the GTID ranges that have server UUIDs that match the given
     * predicate.
     *
     * @param sourceFilter the predicate that returns whether a server UUID is to be included
     * @return the new GtidSet, or this object if {@code sourceFilter} is null; never null
     */
    public GtidSet retainAll(Predicate<String> sourceFilter) {
        if (sourceFilter == null) {
            return this;
        }
        Map<String, UUIDSet> newSets = this.uuidSetsByServerId.entrySet()
                .stream()
                .filter(entry -> sourceFilter.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new GtidSet(newSets);
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
     * Find the {@link UUIDSet} for the server with the specified Uuid.
     *
     * @param uuid the Uuid of the server
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
    public boolean isContainedWithin(GtidSet other) {
        if (other == null) {
            return false;
        }
        if (this.equals(other)) {
            return true;
        }
        for (UUIDSet uuidSet : uuidSetsByServerId.values()) {
            UUIDSet thatSet = other.forServerWithId(uuidSet.getUUID());
            if (!uuidSet.isContainedWithin(thatSet)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Obtain a copy of this {@link GtidSet} except overwritten with all of the GTID ranges in the supplied {@link GtidSet}.
     * @param other the other {@link GtidSet} with ranges to add/overwrite on top of those in this set;
     * @return the new GtidSet, or this object if {@code other} is null or empty; never null
     */
    public GtidSet with(GtidSet other) {
        if (other == null || other.uuidSetsByServerId.isEmpty()) {
            return this;
        }
        Map<String, UUIDSet> newSet = new HashMap<>();
        newSet.putAll(this.uuidSetsByServerId);
        newSet.putAll(other.uuidSetsByServerId);
        return new GtidSet(newSet);
    }

    /**
     * Returns a copy with all intervals set to beginning
     * @return
     */
    public GtidSet getGtidSetBeginning() {
        Map<String, UUIDSet> newSet = new HashMap<>();

        for (UUIDSet uuidSet : uuidSetsByServerId.values()) {
            newSet.put(uuidSet.getUUID(), uuidSet.asIntervalBeginning());
        }

        return new GtidSet(newSet);
    }

    public boolean contains(String gtid) {
        String[] split = GTID_DELIMITER.split(gtid);
        String sourceId = split[0];
        UUIDSet uuidSet = forServerWithId(sourceId);
        if (uuidSet == null) {
            return false;
        }
        long transactionId = Long.parseLong(split[1]);
        return uuidSet.contains(transactionId);
    }

    public GtidSet subtract(GtidSet other) {
        if (other == null) {
            return this;
        }
        Map<String, UUIDSet> newSets = this.uuidSetsByServerId.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isContainedWithin(other.forServerWithId(entry.getKey())))
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().subtract(other.forServerWithId(entry.getKey()))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new GtidSet(newSets);
    }

    @Override
    public int hashCode() {
        return uuidSetsByServerId.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.uuidSetsByServerId.equals(that.uuidSetsByServerId);
        }
        return false;
    }

    @Override
    public String toString() {
        List<String> gtids = new ArrayList<String>();
        for (UUIDSet uuidSet : uuidSetsByServerId.values()) {
            gtids.add(uuidSet.toString());
        }
        return String.join(",", gtids);
    }

    /**
     * A range of GTIDs for a single server with a specific Uuid.
     */
    @Immutable
    public static class UUIDSet {

        private final String uuid;
        private final LinkedList<Interval> intervals = new LinkedList<>();

        protected UUIDSet(com.github.shyiko.mysql.binlog.GtidSet.UUIDSet uuidSet) {
            this.uuid = uuidSet.getUUID();
            uuidSet.getIntervals().forEach(interval -> {
                intervals.add(new Interval(interval.getStart(), interval.getEnd()));
            });
            Collections.sort(this.intervals);
            if (this.intervals.size() > 1) {
                // Collapse adjacent intervals ...
                for (int i = intervals.size() - 1; i != 0; --i) {
                    Interval before = this.intervals.get(i - 1);
                    Interval after = this.intervals.get(i);
                    if ((before.getEnd() + 1) == after.getStart()) {
                        this.intervals.set(i - 1, new Interval(before.getStart(), after.getEnd()));
                        this.intervals.remove(i);
                    }
                }
            }
        }

        protected UUIDSet(String uuid, Interval interval) {
            this.uuid = uuid;
            this.intervals.add(interval);
        }

        protected UUIDSet(String uuid, List<Interval> intervals) {
            this.uuid = uuid;
            this.intervals.addAll(intervals);
        }

        public UUIDSet asIntervalBeginning() {
            Interval start = new Interval(intervals.get(0).getStart(), intervals.get(0).getStart());
            return new UUIDSet(this.uuid, start);
        }

        /**
         * Get the Uuid for the server that generated the GTIDs.
         *
         * @return the server's Uuid; never null
         */
        public String getUUID() {
            return uuid;
        }

        /**
         * Get the intervals of transaction numbers.
         *
         * @return the immutable transaction intervals; never null
         */
        public List<Interval> getIntervals() {
            return Collections.unmodifiableList(intervals);
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction numbers from
         * the set of transaction numbers in the supplied set.
         *
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are a subset of the transaction numbers of the supplied set,
         *         or false otherwise
         */
        public boolean isContainedWithin(UUIDSet other) {
            if (other == null) {
                return false;
            }
            if (!this.getUUID().equalsIgnoreCase(other.getUUID())) {
                // Not even the same server ...
                return false;
            }
            if (this.intervals.isEmpty()) {
                return true;
            }
            if (other.intervals.isEmpty()) {
                return false;
            }
            assert this.intervals.size() > 0;
            assert other.intervals.size() > 0;

            // Every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isContainedWithin(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false; // didn't find a match
                }
            }
            return true;
        }

        public boolean contains(long transactionId) {
            for (Interval interval : this.intervals) {
                if (interval.contains(transactionId)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return uuid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.getUUID().equalsIgnoreCase(that.getUUID()) && this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(uuid).append(':');
            Iterator<Interval> iter = intervals.iterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                sb.append(iter.next());
            }
            return sb.toString();
        }

        public UUIDSet subtract(UUIDSet other) {
            if (!uuid.equals(other.getUUID())) {
                throw new IllegalArgumentException("UUIDSet subtraction is supported only within a single server UUID");
            }
            List<Interval> result = new ArrayList<>();
            for (Interval interval : intervals) {
                result.addAll(interval.removeAll(other.getIntervals()));
            }
            return new UUIDSet(uuid, result);
        }
    }

    @Immutable
    public static class Interval implements Comparable<Interval> {

        private final long start;
        private final long end;

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         *
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         *
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         *
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         *         {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied interval's
         *         {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isContainedWithin(Interval other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        public boolean contains(long transactionId) {
            return getStart() <= transactionId && transactionId <= getEnd();
        }

        public boolean contains(Interval other) {
            return getStart() <= other.getStart() && getEnd() >= other.getEnd();
        }

        public boolean nonintersecting(Interval other) {
            return other.getEnd() < this.getStart() || other.getStart() > this.getEnd();
        }

        public List<Interval> remove(Interval other) {
            if (nonintersecting(other)) {
                return Collections.singletonList(this);
            }
            if (other.contains(this)) {
                return Collections.emptyList();
            }
            List<Interval> result = new LinkedList<>();
            if (this.getStart() < other.getStart()) {
                Interval part = new Interval(this.getStart(), other.getStart() - 1);
                result.add(part);
            }
            if (other.getEnd() < this.getEnd()) {
                Interval part = new Interval(other.getEnd() + 1, this.getEnd());
                result.add(part);
            }
            return result;
        }

        public List<Interval> removeAll(List<Interval> otherIntervals) {
            List<Interval> thisIntervals = new LinkedList<>();
            thisIntervals.add(this);
            List<Interval> result = new LinkedList<>();
            result.add(this);
            for (Interval other : otherIntervals) {
                result = new LinkedList<>();
                for (Interval thisInterval : thisIntervals) {
                    result.addAll(thisInterval.remove(other));
                }
                thisIntervals = result;
            }
            return result;
        }

        @Override
        public int compareTo(Interval that) {
            if (that == this) {
                return 0;
            }
            long diff = this.start - that.start;
            if (diff > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (diff < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
            return (int) diff;
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Interval) {
                Interval that = (Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            return "" + getStart() + "-" + getEnd();
        }
    }
}
