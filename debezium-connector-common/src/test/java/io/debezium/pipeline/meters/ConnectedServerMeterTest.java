/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ConnectedServerMeter}.
 */
public class ConnectedServerMeterTest {

    private static final Map<String, String> SERVER_1 = Map.of("server_id", "1");
    private static final Map<String, String> SERVER_2 = Map.of("server_id", "2");

    private final ConnectedServerMeter meter = new ConnectedServerMeter();

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
}
