/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;

class CompositeHeartbeatTest {

    @Test
    @FixFor("debezium/dbz#2906")
    void shouldTreatScheduledHeartbeatAsEnabledWhenNoExtraHeartbeatsAreConfigured() {
        final ScheduledHeartbeat scheduled = () -> true;
        final var heartbeat = new CompositeHeartbeat(scheduled, List.of());

        assertThat(heartbeat.isEnabled()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2906")
    void shouldStayDisabledWhenTheScheduledHeartbeatIsOff() {
        final var heartbeat = new CompositeHeartbeat(ScheduledHeartbeat.NOOP_HEARTBEAT, List.of());

        assertThat(heartbeat.isEnabled()).isFalse();
    }
}
