/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.pipeline.source.spi.ServerRole;
import io.debezium.pipeline.source.spi.ServerRoleObservation;

/**
 * The facts that the role of a MySQL server is determined from.
 *
 * @param readOnly the value of {@code read_only}, which {@code super_read_only} implies
 * @param superReadOnly the value of {@code super_read_only}
 * @param auroraReader on Amazon Aurora MySQL, the value of {@code innodb_read_only}, which tells a reader from the
 *            writer; null on other servers
 * @param groupReplicationRole the {@code MEMBER_ROLE} of the server if it is an online member of a replication group;
 *            null otherwise
 * @param replicationSources the {@code host:port} of the sources that the running replication channels replicate from,
 *            other than the channels of Group Replication; never null
 */
public record MySqlServerRoleFacts(boolean readOnly, boolean superReadOnly, Boolean auroraReader, String groupReplicationRole,
        List<String> replicationSources) {

    static final String SOURCE_AURORA = "AURORA";
    static final String SOURCE_GROUP_REPLICATION = "GROUP_REPLICATION";
    static final String SOURCE_INFERRED = "INFERRED";

    /**
     * Determines the role of the server. Amazon Aurora MySQL and Group Replication report the role explicitly. Other
     * servers do not, so a server that replicates from another one is taken for a replica, and a writable server that
     * does not replicate for the primary, which the source {@code INFERRED} tells. A read-only server that does not
     * replicate has no role that can be told, for example a former primary that was fenced off after a failover.
     *
     * @return the observed role; never null
     */
    public ServerRoleObservation toObservation() {
        final Map<String, String> state = new HashMap<>();
        state.put("read_only", Boolean.toString(readOnly));
        state.put("super_read_only", Boolean.toString(superReadOnly));
        if (auroraReader != null) {
            state.put("innodb_read_only", Boolean.toString(auroraReader));
            return new ServerRoleObservation(auroraReader ? ServerRole.REPLICA : ServerRole.PRIMARY, SOURCE_AURORA, state);
        }
        if (groupReplicationRole != null) {
            state.put("group_replication_member_role", groupReplicationRole);
            return switch (groupReplicationRole) {
                case "PRIMARY" -> new ServerRoleObservation(ServerRole.PRIMARY, SOURCE_GROUP_REPLICATION, state);
                case "SECONDARY" -> new ServerRoleObservation(ServerRole.REPLICA, SOURCE_GROUP_REPLICATION, state);
                default -> new ServerRoleObservation(ServerRole.UNKNOWN, "", state);
            };
        }
        if (!replicationSources.isEmpty()) {
            state.put("replication_sources", String.join(",", replicationSources));
            return new ServerRoleObservation(ServerRole.REPLICA, SOURCE_INFERRED, state);
        }
        if (!readOnly) {
            return new ServerRoleObservation(ServerRole.PRIMARY, SOURCE_INFERRED, state);
        }
        return new ServerRoleObservation(ServerRole.UNKNOWN, "", state);
    }
}
