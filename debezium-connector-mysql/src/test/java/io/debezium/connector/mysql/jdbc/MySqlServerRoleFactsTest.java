/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.ServerRole;
import io.debezium.pipeline.source.spi.ServerRoleObservation;

/**
 * Unit tests for {@link MySqlServerRoleFacts}.
 */
public class MySqlServerRoleFactsTest {

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldInferThePrimaryFromAWritableServerThatDoesNotReplicate() {
        final ServerRoleObservation observation = new MySqlServerRoleFacts(false, false, null, null, List.of()).toObservation();

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("INFERRED");
        assertThat(observation.state()).containsEntry("read_only", "false").containsEntry("super_read_only", "false");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldInferAReplicaFromARunningReplicationChannel() {
        // Even a writable replica is a replica
        final ServerRoleObservation observation = new MySqlServerRoleFacts(false, false, null, null, List.of("mysql-a:3306")).toObservation();

        assertThat(observation.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(observation.source()).isEqualTo("INFERRED");
        assertThat(observation.state()).containsEntry("replication_sources", "mysql-a:3306");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldNotTellTheRoleOfAReadOnlyServerThatDoesNotReplicate() {
        // For example a former primary that was fenced off after a failover
        final ServerRoleObservation observation = new MySqlServerRoleFacts(true, true, null, null, List.of()).toObservation();

        assertThat(observation.role()).isEqualTo(ServerRole.UNKNOWN);
        assertThat(observation.source()).isEmpty();
        assertThat(observation.state()).containsEntry("read_only", "true").containsEntry("super_read_only", "true");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldUseTheRoleOfAGroupReplicationMember() {
        // A primary of a replication group stays the primary although the group replicates into it
        final ServerRoleObservation primary = new MySqlServerRoleFacts(false, false, null, "PRIMARY", List.of()).toObservation();
        final ServerRoleObservation secondary = new MySqlServerRoleFacts(true, true, null, "SECONDARY", List.of()).toObservation();

        assertThat(primary.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(primary.source()).isEqualTo("GROUP_REPLICATION");
        assertThat(secondary.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(secondary.source()).isEqualTo("GROUP_REPLICATION");
        assertThat(secondary.state()).containsEntry("group_replication_member_role", "SECONDARY");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldUseTheRoleOfAnAuroraInstance() {
        final ServerRoleObservation writer = new MySqlServerRoleFacts(false, false, false, null, List.of()).toObservation();
        final ServerRoleObservation reader = new MySqlServerRoleFacts(false, false, true, null, List.of()).toObservation();

        assertThat(writer.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(writer.source()).isEqualTo("AURORA");
        assertThat(reader.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(reader.source()).isEqualTo("AURORA");
        assertThat(reader.state()).containsEntry("innodb_read_only", "true");
    }
}
