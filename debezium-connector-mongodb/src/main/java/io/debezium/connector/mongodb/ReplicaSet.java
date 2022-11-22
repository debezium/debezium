/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;

import com.mongodb.ConnectionString;

import io.debezium.annotation.Immutable;

@Immutable
public final class ReplicaSet implements Comparable<ReplicaSet> {

    private final String replicaSetName;
    private final ConnectionString connectionString;
    private final int hc;

    public ReplicaSet(String connectionString) {
        this(new ConnectionString(connectionString));
    }

    public ReplicaSet(ConnectionString connectionString) {
        this(connectionString.getRequiredReplicaSetName(), connectionString);
    }

    private ReplicaSet(String replicaSetName, ConnectionString connectionString) {
        this.connectionString = Objects.requireNonNull(connectionString, "Connection string cannot be null");
        this.replicaSetName = Objects.requireNonNull(replicaSetName, "Replica set name cannot be null");
        this.hc = Objects.hash(connectionString);
    }

    /**
     * Get the name of this replica set.
     *
     * @return the replica set name, or {@code null} if the addresses are for standalone servers.
     */
    public String replicaSetName() {
        return replicaSetName;
    }

    /**
     * Get connection string
     *
     * @return connection string for this replica set
     */
    public ConnectionString connectionString() {
        return connectionString;
    }

    /**
     * Return whether the address(es) represents a replica set, where the {@link #replicaSetName() replica set name} is
     * not {@code null}.
     *
     * @return {@code true} if this represents the address of a replica set, or {@code false} if it represents the
     *         address of a standalone server
     */
    public boolean hasReplicaSetName() {
        return replicaSetName != null;
    }

    @Override
    public int hashCode() {
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ReplicaSet) {
            ReplicaSet that = (ReplicaSet) obj;
            return Objects.equals(this.replicaSetName, that.replicaSetName);
        }
        return false;
    }

    @Override
    public int compareTo(ReplicaSet that) {
        if (that == this) {
            return 0;
        }

        return replicaSetName.compareTo(that.replicaSetName);
    }

    @Override
    public String toString() {
        return replicaSetName;
    }

}
