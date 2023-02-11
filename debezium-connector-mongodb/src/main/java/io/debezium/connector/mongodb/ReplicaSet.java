/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.ServerAddress;

import io.debezium.annotation.Immutable;

@Immutable
public final class ReplicaSet implements Comparable<ReplicaSet> {

    /**
     * Regular expression that extracts the hosts for the replica sets. The raw expression is
     * {@code ((([^=]+)[=])?(([^/]+)\/))?(.+)}.
     */
    private static final Pattern HOST_PATTERN = Pattern.compile("((([^=]+)[=])?(([^/]+)\\/))?(.+)");

    /**
     * Parse the supplied string for the information about the hosts for a replica set. The string is a shard host
     * specification (e.g., "{@code shard01=replicaSet1/host1:27017,host2:27017}"), replica set hosts (e.g.,
     * "{@code replicaSet1/host1:27017,host2:27017}"), or standalone host (e.g., "{@code host1:27017}" or
     * "{@code 1.2.3.4:27017}").
     *
     * @param hosts the hosts string; may be null
     * @return the replica set; or {@code null} if the host string could not be parsed
     */
    public static ReplicaSet parse(String hosts) {
        if (hosts != null) {
            Matcher matcher = HOST_PATTERN.matcher(hosts);
            if (matcher.matches()) {
                String shard = matcher.group(3);
                String replicaSetName = matcher.group(5);
                String host = matcher.group(6);
                if (host != null && host.trim().length() != 0) {
                    return new ReplicaSet(host, replicaSetName, shard);
                }
            }
        }
        return null;
    }

    private final List<ServerAddress> addresses;
    private final String replicaSetName;
    private final String shardName;
    private final int hc;

    public ReplicaSet(List<ServerAddress> addresses, String replicaSetName, String shardName) {
        this.addresses = new ArrayList<>(addresses);
        this.addresses.sort(ReplicaSet::compareServerAddresses);
        this.replicaSetName = replicaSetName != null ? replicaSetName.trim() : null;
        this.shardName = shardName != null ? shardName.trim() : null;
        this.hc = addresses.hashCode();
    }

    public ReplicaSet(String addresses, String replicaSetName, String shardName) {
        this(MongoUtil.parseAddresses(addresses), replicaSetName, shardName);
    }

    /**
     * Get the immutable list of server addresses.
     *
     * @return the server addresses; never null
     */
    public List<ServerAddress> addresses() {
        return addresses;
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
     * Get the shard name for this replica set.
     *
     * @return the shard name, or {@code null} if this replica set is not used as a shard
     */
    public String shardName() {
        return shardName;
    }

    /**
     * Return whether the address(es) represent a standalone server, where the {@link #replicaSetName() replica set name} is
     * {@code null}. This method returns the opposite of {@link #hasReplicaSetName()}.
     *
     * @return {@code true} if this represents the address of a standalone server, or {@code false} if it represents the
     *         address of a replica set
     * @see #hasReplicaSetName()
     */
    public boolean isStandaloneServer() {
        return replicaSetName == null;
    }

    /**
     * Return whether the address(es) represents a replica set, where the {@link #replicaSetName() replica set name} is
     * not {@code null}. This method returns the opposite of {@link #isStandaloneServer()}.
     *
     * @return {@code true} if this represents the address of a replica set, or {@code false} if it represents the
     *         address of a standalone server
     * @see #isStandaloneServer()
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
            return Objects.equals(this.shardName, that.shardName) && Objects.equals(this.replicaSetName, that.replicaSetName) &&
                    this.addresses.equals(that.addresses);
        }
        return false;
    }

    @Override
    public int compareTo(ReplicaSet that) {
        if (that == this) {
            return 0;
        }
        int diff = compareNullable(this.shardName, that.shardName);
        if (diff != 0) {
            return diff;
        }
        diff = compareNullable(this.replicaSetName, that.replicaSetName);
        if (diff != 0) {
            return diff;
        }
        Iterator<ServerAddress> thisIter = this.addresses.iterator();
        Iterator<ServerAddress> thatIter = that.addresses.iterator();
        while (thisIter.hasNext() && thatIter.hasNext()) {
            diff = compare(thisIter.next(), thatIter.next());
            if (diff != 0) {
                return diff;
            }
        }
        if (thisIter.hasNext()) {
            return 1;
        }
        if (thatIter.hasNext()) {
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.shardName != null && !this.shardName.isEmpty()) {
            sb.append(shardName).append('=');
        }
        if (this.replicaSetName != null && !this.replicaSetName.isEmpty()) {
            sb.append(replicaSetName).append('/');
        }
        Iterator<ServerAddress> iter = addresses.iterator();
        if (iter.hasNext()) {
            sb.append(MongoUtil.toString(iter.next()));
        }
        while (iter.hasNext()) {
            sb.append(',').append(MongoUtil.toString(iter.next()));
        }
        return sb.toString();
    }

    protected static int compareServerAddresses(ServerAddress one, ServerAddress two) {
        if (one == two) {
            return 0;
        }
        if (one == null) {
            return two == null ? 0 : -1;
        }
        if (two == null) {
            return 1;
        }
        return compare(one, two);
    }

    protected static int compareNullable(String str1, String str2) {
        if (str1 == str2) {
            return 0;
        }
        if (str1 == null) {
            return str2 == null ? 0 : -1;
        }
        if (str2 == null) {
            return 1;
        }
        return str1.compareTo(str2);
    }

    protected static int compare(ServerAddress address1, ServerAddress address2) {
        int diff = address1.getHost().compareTo(address2.getHost());
        if (diff != 0) {
            return diff;
        }
        return address1.getPort() - address2.getPort();
    }

}
