/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.shyiko.mysql.binlog.ServerFacts;

import io.debezium.pipeline.source.spi.ServerRole;
import io.debezium.pipeline.source.spi.ServerRoleObservation;

/**
 * Interprets facts read on the binlog connection as the role of a MySQL or MariaDB server.
 * The client collects facts; the connector interprets them in the context of the topology.
 */
final class BinlogServerRoleFacts {

    private BinlogServerRoleFacts() {
    }

    static ServerRoleObservation observe(ServerFacts facts) {
        if (facts == null) {
            return ServerRoleObservation.UNKNOWN;
        }
        final Map<String, String> state = new HashMap<>();
        if (facts.getReadOnly() != null) {
            state.put("read_only", facts.getReadOnly().toString());
        }
        if (facts.getSuperReadOnly() != null) {
            state.put("super_read_only", facts.getSuperReadOnly().toString());
        }
        final List<ServerFacts.ReplicationSource> replicationSources = facts.getReplicationSources();
        if (replicationSources != null) {
            state.put("replication_sources", replicationSources.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        // Ordinary MySQL also exposes innodb_read_only. Only Aurora uses it to identify a reader or writer.
        if (facts.getAuroraVersion() != null && !facts.getAuroraVersion().isBlank()) {
            state.put("aurora_version", facts.getAuroraVersion());
            if (facts.getInnodbReadOnly() != null) {
                state.put("innodb_read_only", facts.getInnodbReadOnly().toString());
                return new ServerRoleObservation(facts.getInnodbReadOnly() ? ServerRole.REPLICA : ServerRole.PRIMARY, "AURORA", state);
            }
            return new ServerRoleObservation(ServerRole.UNKNOWN, "", state);
        }
        if (facts.getGroupReplicationMemberRole() != null) {
            state.put("group_replication_member_role", facts.getGroupReplicationMemberRole());
            final var role = switch (facts.getGroupReplicationMemberRole()) {
                case "PRIMARY" -> ServerRole.PRIMARY;
                case "SECONDARY" -> ServerRole.REPLICA;
                default -> ServerRole.UNKNOWN;
            };
            return new ServerRoleObservation(role, role == ServerRole.UNKNOWN ? "" : "GROUP_REPLICATION", state);
        }
        if (replicationSources != null && !replicationSources.isEmpty()) {
            // A writable server with an active replication source is still a replica.
            return new ServerRoleObservation(ServerRole.REPLICA, "INFERRED", state);
        }
        // Null means that the status could not be read, for example because the statement was rejected.
        // It must not be treated as an empty list, which could misidentify a writable replica as the primary.
        if (replicationSources != null && Boolean.FALSE.equals(facts.getReadOnly()) && !Boolean.TRUE.equals(facts.getSuperReadOnly())) {
            return new ServerRoleObservation(ServerRole.PRIMARY, "INFERRED", state);
        }
        return new ServerRoleObservation(ServerRole.UNKNOWN, "", state);
    }
}
