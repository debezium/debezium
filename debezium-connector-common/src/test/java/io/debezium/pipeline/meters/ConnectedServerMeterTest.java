/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.ServerRole;
import io.debezium.pipeline.source.spi.ServerRoleObservation;

/**
 * Unit tests for {@link ConnectedServerMeter}.
 */
public class ConnectedServerMeterTest {

    private static final Map<String, String> SERVER_1 = Map.of("server_id", "1");
    private static final Map<String, String> SERVER_2 = Map.of("server_id", "2");
    private static final ServerRoleObservation PRIMARY = new ServerRoleObservation(ServerRole.PRIMARY, "INFERRED", Map.of("read_only", "false"));
    private static final ServerRoleObservation REPLICA = new ServerRoleObservation(ServerRole.REPLICA, "INFERRED", Map.of("read_only", "true"));

    private final AtomicLong now = new AtomicLong(1_000);
    private final ConnectedServerMeter meter = new ConnectedServerMeter(now::get);

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldReportNoServerBeforeTheFirstConnection() {
        assertThat(meter.getConnectedServer()).isEmpty();
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldNotCountTheFirstConnection() {
        meter.connected(SERVER_1);

        assertThat(meter.getConnectedServer()).isEqualTo(SERVER_1);
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldNotCountReconnectionsToTheSameServer() {
        meter.connected(SERVER_1);
        meter.disconnected();
        meter.connected(SERVER_1);

        assertThat(meter.getConnectedServer()).isEqualTo(SERVER_1);
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldCountEachConnectionToAnotherServer() {
        meter.connected(SERVER_1);
        meter.disconnected();
        meter.connected(SERVER_2);
        meter.disconnected();
        meter.connected(SERVER_1);

        assertThat(meter.getConnectedServer()).isEqualTo(SERVER_1);
        assertThat(meter.getNumberOfConnectedServerChanges()).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldReportNoServerWhileDisconnected() {
        meter.connected(SERVER_1);
        meter.disconnected();

        assertThat(meter.getConnectedServer()).isEmpty();
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldNotCompareUnknownServers() {
        meter.connected(SERVER_1);
        meter.connected(Map.of());

        assertThat(meter.getConnectedServer()).isEmpty();
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();

        // The unknown server is not remembered, so the next connection is compared with the last known one
        meter.connected(SERVER_2);

        assertThat(meter.getNumberOfConnectedServerChanges()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldResetTheNumberOfChangesButKeepThePreviousServer() {
        meter.connected(SERVER_1);
        meter.connected(SERVER_2);
        meter.reset();

        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
        assertThat(meter.getConnectedServer()).isEqualTo(SERVER_2);

        meter.connected(SERVER_1);

        assertThat(meter.getNumberOfConnectedServerChanges()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldReportAnUnknownRoleUntilItIsObserved() {
        meter.connected(SERVER_1);

        assertThat(meter.getConnectedServerRole()).isEqualTo("UNKNOWN");
        assertThat(meter.getConnectedServerRoleSource()).isEmpty();
        assertThat(meter.getConnectedServerState()).isEmpty();
        assertThat(meter.getMilliSecondsSinceConnectedServerRoleObserved()).isEqualTo(-1);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldReportTheObservedRoleOfTheConnectedServer() {
        meter.connected(SERVER_1);
        meter.roleObserved(SERVER_1, PRIMARY);
        now.addAndGet(250);

        assertThat(meter.getConnectedServerRole()).isEqualTo("PRIMARY");
        assertThat(meter.getConnectedServerRoleSource()).isEqualTo("INFERRED");
        assertThat(meter.getConnectedServerState()).isEqualTo(Map.of("read_only", "false"));
        assertThat(meter.getMilliSecondsSinceConnectedServerRoleObserved()).isEqualTo(250);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldIgnoreTheRoleObservedForAnotherServer() {
        meter.connected(SERVER_2);
        // Observed just before the connector reconnected to server 2
        meter.roleObserved(SERVER_1, PRIMARY);

        assertThat(meter.getConnectedServerRole()).isEqualTo("UNKNOWN");
        assertThat(meter.getMilliSecondsSinceConnectedServerRoleObserved()).isEqualTo(-1);
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldForgetTheRoleWhenTheConnectionIsLostOrEstablishedAgain() {
        meter.connected(SERVER_1);
        meter.roleObserved(SERVER_1, PRIMARY);
        meter.disconnected();

        assertThat(meter.getConnectedServerRole()).isEqualTo("UNKNOWN");
        assertThat(meter.getConnectedServerState()).isEmpty();

        meter.roleObserved(SERVER_1, PRIMARY);

        assertThat(meter.getConnectedServerRole()).isEqualTo("UNKNOWN");

        meter.connected(SERVER_1);

        assertThat(meter.getConnectedServerRole()).isEqualTo("UNKNOWN");
    }

    @Test
    @FixFor("debezium/dbz#2969")
    public void shouldNotCountARoleChangeAsAServerChange() {
        meter.connected(SERVER_1);
        meter.roleObserved(SERVER_1, REPLICA);
        // The server is promoted while the connector stays connected
        meter.roleObserved(SERVER_1, PRIMARY);

        assertThat(meter.getConnectedServerRole()).isEqualTo("PRIMARY");
        assertThat(meter.getNumberOfConnectedServerChanges()).isZero();
    }
}
