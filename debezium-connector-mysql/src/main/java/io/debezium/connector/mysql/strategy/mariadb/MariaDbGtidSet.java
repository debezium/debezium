/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.MariadbGtidSet.MariaGtid;

import io.debezium.connector.mysql.GtidSet;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public class MariaDbGtidSet implements GtidSet {

    private Map<MariaDbGtidStreamId, MariaDbStreamSet> streamSets = new TreeMap<>();

    public MariaDbGtidSet(String gtidSet) {
        if (gtidSet != null && !gtidSet.isEmpty()) {
            String[] gtids = gtidSet.replaceAll("\n", "").split(",");
            Arrays.stream(gtids).forEach(gtid -> {
                if (MariadbGtidSet.isMariaGtidSet(gtid)) {
                    MariaDbGtid mariaGtid = MariaDbGtid.parse(gtid);
                    MariaDbGtidStreamId streamId = new MariaDbGtidStreamId(mariaGtid);
                    if (!streamSets.containsKey(streamId)) {
                        streamSets.put(streamId, new MariaDbStreamSet());
                    }
                    streamSets.get(streamId).add(mariaGtid);
                }
            });
        }
    }

    protected MariaDbGtidSet(Map<MariaDbGtidStreamId, MariaDbStreamSet> streamSets) {
        this.streamSets = streamSets;
    }

    @Override
    public boolean isEmpty() {
        return streamSets.isEmpty();
    }

    @Override
    public GtidSet retainAll(Predicate<String> sourceFilter) {
        if (sourceFilter == null) {
            return this;
        }

        Map<MariaDbGtidStreamId, MariaDbStreamSet> newSets = streamSets.entrySet()
                .stream()
                .filter(entry -> sourceFilter.test(entry.getKey().asSourceFilterValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new MariaDbGtidSet(newSets);
    }

    @Override
    public boolean isContainedWithin(GtidSet other) {
        if (!(other instanceof MariaDbGtidSet)) {
            return false;
        }
        if (this.equals(other)) {
            return true;
        }

        final MariaDbGtidSet theOther = (MariaDbGtidSet) other;
        for (Map.Entry<MariaDbGtidStreamId, MariaDbStreamSet> entry : streamSets.entrySet()) {
            MariaDbStreamSet thatSet = theOther.forStreamId(entry.getKey());
            if (!entry.getValue().isContainedWith(thatSet)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public GtidSet with(GtidSet other) {
        final MariaDbGtidSet theOther = (MariaDbGtidSet) other;
        if (theOther == null || theOther.streamSets.isEmpty()) {
            return this;
        }

        final Map<MariaDbGtidStreamId, MariaDbStreamSet> newSet = new HashMap<>();
        newSet.putAll(streamSets);
        newSet.putAll(theOther.streamSets);
        return new MariaDbGtidSet(newSet);
    }

    @Override
    public GtidSet getGtidSetBeginning() {
        final Map<MariaDbGtidStreamId, MariaDbStreamSet> newSet = new HashMap<>();
        for (Map.Entry<MariaDbGtidStreamId, MariaDbStreamSet> streamSet : streamSets.entrySet()) {
            newSet.put(streamSet.getKey(), streamSet.getValue().asBeginning());
        }
        return new MariaDbGtidSet(newSet);
    }

    @Override
    public boolean contains(String gtid) {
        if (!MariadbGtidSet.isMariaGtidSet(gtid)) {
            return false;
        }

        final MariaDbGtid mariaGtid = MariaDbGtid.parse(gtid);
        final Set<MariaDbGtid> streamSet = forGtidStream(mariaGtid);
        if (streamSet == null) {
            return false;
        }

        return streamSet.contains(mariaGtid);
    }

    @Override
    public MariaDbGtidSet subtract(GtidSet other) {
        if (other == null) {
            return this;
        }

        final MariaDbGtidSet theOther = (MariaDbGtidSet) other;
        Map<MariaDbGtidStreamId, MariaDbStreamSet> newSets = streamSets.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isContainedWith(theOther.forStreamId(entry.getKey())))
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().subtract(theOther.forStreamId(entry.getKey()))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new MariaDbGtidSet(newSets);
    }

    public boolean isKnown(MariaDbGtid gtid) {
        final MariaDbStreamSet streamSet = forGtidStream(gtid);
        if (streamSet == null) {
            return false;
        }
        return streamSet.hasSequence(gtid.getSequence());
    }

    public MariaDbStreamSet forGtidStream(MariaDbGtid gtid) {
        return forStreamId(new MariaDbGtidStreamId(gtid));
    }

    public MariaDbStreamSet forStreamId(MariaDbGtidStreamId streamId) {
        return streamSets.get(streamId);
    }

    public static MariaDbGtid parse(String gtid) {
        if (Strings.isNullOrBlank(gtid)) {
            throw new IllegalStateException("Cannot parse empty GTID");
        }
        return MariaDbGtid.parse(gtid);
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        }
        if (value == null || getClass() != value.getClass()) {
            return false;
        }
        MariaDbGtidSet that = (MariaDbGtidSet) value;
        return Objects.equals(streamSets, that.streamSets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamSets);
    }

    @Override
    public String toString() {
        return streamSets.values().stream().map(MariaDbStreamSet::toString).collect(Collectors.joining(","));
    }

    public static class MariaDbGtidStreamId implements Comparable<MariaDbGtidStreamId> {

        private final long domainId;
        private final long serverId;

        public MariaDbGtidStreamId(long domainId, long serverId) {
            this.domainId = domainId;
            this.serverId = serverId;
        }

        public MariaDbGtidStreamId(MariaDbGtid gtid) {
            this(gtid.getDomainId(), gtid.getServerId());
        }

        public long getDomainId() {
            return domainId;
        }

        public long getServerId() {
            return serverId;
        }

        public String asSourceFilterValue() {
            return String.format("%d-%d", domainId, serverId);
        }

        public boolean isSameDomainAndServer(MariaGtid gtid) {
            return gtid.getDomainId() == domainId && gtid.getServerId() == serverId;
        }

        @Override
        public boolean equals(Object value) {
            if (this == value) {
                return true;
            }
            if (value == null || getClass() != value.getClass()) {
                return false;
            }
            MariaDbGtidStreamId that = (MariaDbGtidStreamId) value;
            return domainId == that.domainId && serverId == that.serverId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(domainId, serverId);
        }

        @Override
        public int compareTo(MariaDbGtidStreamId other) {
            final int domainComparison = Long.compare(domainId, other.domainId);
            if (domainComparison == 0) {
                return Long.compare(serverId, other.serverId);
            }
            return domainComparison;
        }

        @Override
        public String toString() {
            return "MariaDbGtidStreamId{" +
                    "domainId=" + domainId +
                    ", serverId=" + serverId +
                    '}';
        }
    }

    public static class MariaDbStreamSet extends TreeSet<MariaDbGtid> {

        public boolean hasSequence(long sequence) {
            for (MariaDbGtid gtid : this) {
                if (gtid.getSequence() == sequence) {
                    return true;
                }
            }
            return false;
        }

        @SuppressWarnings("all")
        public boolean isContainedWith(MariaDbStreamSet other) {
            if (other == null) {
                return false;
            }
            if (other.containsAll(this)) {
                return true;
            }
            return isAllBefore(other);
        }

        public boolean isAllBefore(MariaDbStreamSet other) {
            final Long otherMinSequence = other.stream().mapToLong(MariaDbGtid::getSequence).min().getAsLong();
            final Long minSequence = stream().mapToLong(MariaDbGtid::getSequence).min().getAsLong();
            return minSequence <= otherMinSequence;
        }

        public MariaDbStreamSet subtract(MariaDbStreamSet other) {
            if (other == null) {
                return this;
            }
            final MariaDbStreamSet streamSet = new MariaDbStreamSet();
            streamSet.addAll(stream().filter(gtid -> !other.contains(gtid)).collect(Collectors.toSet()));
            return streamSet;
        }

        public MariaDbStreamSet asBeginning() {
            MariaDbStreamSet newSet = new MariaDbStreamSet();
            if (!isEmpty()) {
                newSet.add(newSet.first());
            }
            return newSet;
        }

        @Override
        public String toString() {
            return stream().map(MariaDbGtid::toString).collect(Collectors.joining(","));
        }
    }

    public static class MariaDbGtid implements Comparable<MariaDbGtid> {

        private final long domainId;
        private final long serverId;
        private final long sequence;

        public MariaDbGtid(MariaGtid gtid) {
            this.domainId = gtid.getDomainId();
            this.serverId = gtid.getServerId();
            this.sequence = gtid.getSequence();
        }

        public long getDomainId() {
            return domainId;
        }

        public long getServerId() {
            return serverId;
        }

        public long getSequence() {
            return sequence;
        }

        public static MariaDbGtid parse(String gtid) {
            return new MariaDbGtid(MariaGtid.parse(gtid));
        }

        @Override
        public boolean equals(Object value) {
            if (this == value) {
                return true;
            }
            if (value == null || getClass() != value.getClass()) {
                return false;
            }
            MariaDbGtid that = (MariaDbGtid) value;
            return domainId == that.domainId && serverId == that.serverId && sequence == that.sequence;
        }

        @Override
        public int hashCode() {
            return Objects.hash(domainId, serverId, sequence);
        }

        @Override
        public int compareTo(MariaDbGtid other) {
            final int domainComparison = Long.compare(domainId, other.domainId);
            if (domainComparison == 0) {
                final int serverComparison = Long.compare(serverId, other.serverId);
                if (serverComparison == 0) {
                    return Long.compare(sequence, other.sequence);
                }
                return serverComparison;
            }
            return domainComparison;
        }

        @Override
        public String toString() {
            return domainId + "-" + serverId + "-" + sequence;
        }
    }
}
