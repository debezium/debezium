/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.connect.util.ConnectorUtils;

import io.debezium.annotation.Immutable;
import io.debezium.util.Strings;

/**
 * A set of replica set specifications.
 *
 * @author Randall Hauch
 */
@Immutable
public class ReplicaSets {

    private static final Pattern REPLICA_DELIMITER_PATTERN = Pattern.compile(";");

    /**
     * Parse the supplied string for the information about the replica set hosts. The string is a semicolon-delimited list of
     * shard hosts (e.g., "{@code shard01=replicaSet1/host1:27017,host2:27017}"), replica set hosts (e.g.,
     * "{@code replicaSet1/host1:27017,host2:27017}"), and standalone hosts (e.g., "{@code host1:27017}" or
     * "{@code 1.2.3.4:27017}").
     *
     * @param hosts the hosts string; may be null
     * @return the replica sets; never null but possibly empty
     * @see ReplicaSets#hosts()
     */
    public static ReplicaSets parse(String hosts) {
        Set<ReplicaSet> replicaSets = new HashSet<>();
        if (hosts != null) {
            for (String replicaSetStr : REPLICA_DELIMITER_PATTERN.split(hosts.trim())) {
                if (!replicaSetStr.isEmpty()) {
                    ReplicaSet replicaSet = ReplicaSet.parse(replicaSetStr);
                    if (replicaSetStr != null) {
                        replicaSets.add(replicaSet);
                    }
                }
            }
        }
        return new ReplicaSets(replicaSets);
    }

    /**
     * Get an instance that contains no replica sets.
     *
     * @return the empty instance; never null
     */
    public static ReplicaSets empty() {
        return new ReplicaSets(null);
    }

    private final Map<String, ReplicaSet> replicaSetsByName = new HashMap<>();
    private final List<ReplicaSet> nonReplicaSets = new ArrayList<>();

    /**
     * Create a set of replica set specifications.
     *
     * @param rsSpecs the replica set specifications; may be null or empty
     */
    public ReplicaSets(Collection<ReplicaSet> rsSpecs) {
        if (rsSpecs != null) {
            rsSpecs.forEach(replicaSet -> {
                if (replicaSet.hasReplicaSetName()) {
                    replicaSetsByName.put(replicaSet.replicaSetName(), replicaSet);
                }
                else {
                    nonReplicaSets.add(replicaSet);
                }
            });
        }
        Collections.sort(nonReplicaSets);
    }

    /**
     * Get the number of replica sets.
     *
     * @return the replica set count
     */
    public int replicaSetCount() {
        return replicaSetsByName.size() + nonReplicaSets.size();
    }

    /**
     * Get the number of replica sets with names.
     *
     * @return the valid replica set count
     */
    public int validReplicaSetCount() {
        return replicaSetsByName.size();
    }

    /**
     * Perform the supplied function on each of the replica sets
     *
     * @param function the consumer function; may not be null
     */
    public void onEachReplicaSet(Consumer<ReplicaSet> function) {
        this.replicaSetsByName.values().forEach(function);
        this.nonReplicaSets.forEach(function);
    }

    /**
     * Subdivide this collection of replica sets into the maximum number of groups.
     *
     * @param maxSubdivisionCount the maximum number of subdivisions
     * @param subdivisionConsumer the function to be called with each subdivision; may not be null
     */
    public void subdivide(int maxSubdivisionCount, Consumer<ReplicaSets> subdivisionConsumer) {
        int numGroups = Math.min(replicaSetCount(), maxSubdivisionCount);
        if (numGroups <= 1) {
            // Just one replica set or subdivision ...
            subdivisionConsumer.accept(this);
            return;
        }
        ConnectorUtils.groupPartitions(all(), numGroups).forEach(rsList -> {
            subdivisionConsumer.accept(new ReplicaSets(rsList));
        });
    }

    /**
     * Assign the replset to the consumer.
     * Currently, should only be called when there is only one replset.
     *
     * @param numTasks the number of tasks to assign this replset to
     * @param consumer the function to be called for each task; may not be null
     */
    public void assignToMultiTasks(int numTasks, Consumer<ReplicaSets> consumer) {
        for (int i = 0; i < numTasks; i++) {
            consumer.accept(this);
        }
    }

    /**
     * Get a copy of all of the {@link ReplicaSet} objects.
     *
     * @return the replica set objects; never null but possibly empty
     */
    public List<ReplicaSet> all() {
        List<ReplicaSet> replicaSets = new ArrayList<>();
        replicaSets.addAll(replicaSetsByName.values());
        replicaSets.addAll(nonReplicaSets);
        return replicaSets;
    }

    /**
     * Get a copy of all of the valid {@link ReplicaSet} objects that have names.
     *
     * @return the valid replica set objects; never null but possibly empty
     */
    public List<ReplicaSet> validReplicaSets() {
        List<ReplicaSet> replicaSets = new ArrayList<>();
        replicaSets.addAll(replicaSetsByName.values());
        return replicaSets;
    }

    /**
     * Get a copy of all of the {@link ReplicaSet} objects that have no names.
     *
     * @return the unnamed replica set objects; never null but possibly empty
     */
    public List<ReplicaSet> unnamedReplicaSets() {
        List<ReplicaSet> replicaSets = new ArrayList<>();
        replicaSets.addAll(nonReplicaSets);
        return replicaSets;
    }

    /**
     * Determine if one or more replica sets has been added or removed since the prior state.
     *
     * @param priorState the prior state of the replica sets; may be null
     * @return {@code true} if the replica sets have changed since the prior state, or {@code false} otherwise
     */
    public boolean haveChangedSince(ReplicaSets priorState) {
        if (priorState.replicaSetCount() != this.replicaSetCount()) {
            // At least one replica set has been added or removed ...
            return true;
        }
        if (this.replicaSetsByName.size() != priorState.replicaSetsByName.size()) {
            // The total number of replica sets hasn't changed, but the number of named replica sets has changed ...
            return true;
        }
        // We have the same number of named replica sets ...
        if (!this.replicaSetsByName.isEmpty()) {
            if (!this.replicaSetsByName.keySet().equals(priorState.replicaSetsByName.keySet())) {
                // The replica sets have different names ...
                return true;
            }
            // Otherwise, they have the same names and we don't care about the members ...
        }
        // None of the named replica sets has changed, so we have no choice to be compare the non-replica set members ...
        return this.nonReplicaSets.equals(priorState.nonReplicaSets) ? false : true;
    }

    /**
     * Get the string containing the host names for the replica sets. The result is a string with each replica set hosts
     * separated by a semicolon.
     *
     * @return the host names; never null
     * @see #parse(String)
     */
    public String hosts() {
        return Strings.join(";", all());
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicaSetsByName, nonReplicaSets);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ReplicaSets) {
            ReplicaSets that = (ReplicaSets) obj;
            return this.replicaSetsByName.equals(that.replicaSetsByName) && this.nonReplicaSets.equals(that.nonReplicaSets);
        }
        return false;
    }

    @Override
    public String toString() {
        return hosts();
    }

}
