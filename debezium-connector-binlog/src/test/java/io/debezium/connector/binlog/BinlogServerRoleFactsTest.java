/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.ServerFacts;
import com.github.shyiko.mysql.binlog.ServerFacts.ReplicationSource;

import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.ServerRole;

/**
 * Verifies role interpretation without a database or timing dependencies.
 */
class BinlogServerRoleFactsTest {

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldReportUnknownWhenFactsAreUnavailable() {
        assertThat(BinlogServerRoleFacts.observe(null).role()).isEqualTo(ServerRole.UNKNOWN);
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(null, null, null, null, null, null)).state()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldInferThePrimaryFromAWritableServerWithoutReplicationSources() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, null, null, null, List.of()));

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("INFERRED");
        assertThat(observation.state()).containsEntry("read_only", "false").containsEntry("super_read_only", "false");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotTreatAnUnavailableReplicaStatusAsNoReplicationSources() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, null, null, null));

        assertThat(observation.role()).isEqualTo(ServerRole.UNKNOWN);
        assertThat(observation.source()).isEmpty();
        assertThat(observation.state()).containsEntry("read_only", "false").doesNotContainKey("replication_sources");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotAssumeThatAnUnknownReadOnlyFlagMeansWritable() {
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(null, false, null, null, null, List.of())).role())
                .isEqualTo(ServerRole.UNKNOWN);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotInferThePrimaryWhenSuperReadOnlyIsEnabled() {
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(false, true, null, null, null, List.of())).role())
                .isEqualTo(ServerRole.UNKNOWN);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotInferTheRoleOfAReadOnlyServerWithoutReplicationSources() {
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(true, true, false, null, null, List.of())).role())
                .isEqualTo(ServerRole.UNKNOWN);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldInferAReplicaEvenWhenItIsWritable() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, null, null,
                List.of(new ReplicationSource("channel", "mysql-a", 3306))));

        assertThat(observation.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(observation.source()).isEqualTo("INFERRED");
        assertThat(observation.state()).containsEntry("replication_sources", "channel=mysql-a:3306");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldInferAReplicaWithoutReadOnlyFlags() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(null, null, null, null, null,
                List.of(new ReplicationSource("", "mysql-a", 3306))));

        assertThat(observation.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(observation.state()).doesNotContainKeys("read_only", "super_read_only");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotIdentifyOrdinaryMySqlAsAuroraFromInnodbReadOnly() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, null, null, List.of()));

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("INFERRED");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldUseTheRoleOfAnAuroraWriter() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, "3.05.2", null, null));

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("AURORA");
        assertThat(observation.state()).containsEntry("innodb_read_only", "false");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldUseTheRoleOfAnAuroraReader() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(true, true, true, "3.05.2", null, null));

        assertThat(observation.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(observation.source()).isEqualTo("AURORA");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldReportUnknownWhenTheAuroraRoleCannotBeRead() {
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(false, false, null, "3.05.2", null, List.of())).role())
                .isEqualTo(ServerRole.UNKNOWN);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldPreferTheGroupReplicationRoleOverReplicationSources() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, null, "PRIMARY",
                List.of(new ReplicationSource("async", "mysql-a", 3306))));

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("GROUP_REPLICATION");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldUseTheRoleOfAGroupReplicationSecondary() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(true, true, false, null, "SECONDARY", List.of()));

        assertThat(observation.role()).isEqualTo(ServerRole.REPLICA);
        assertThat(observation.state()).containsEntry("group_replication_member_role", "SECONDARY");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldNotInferThePrimaryFromAnUnknownGroupReplicationRole() {
        assertThat(BinlogServerRoleFacts.observe(new ServerFacts(false, false, false, null, "UNKNOWN", List.of())).role())
                .isEqualTo(ServerRole.UNKNOWN);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldSupportMariaDbWithoutSuperReadOnly() {
        final var observation = BinlogServerRoleFacts.observe(new ServerFacts(false, null, false, null, null, List.of()));

        assertThat(observation.role()).isEqualTo(ServerRole.PRIMARY);
        assertThat(observation.source()).isEqualTo("INFERRED");
        assertThat(observation.state()).doesNotContainKey("super_read_only");
    }
}
