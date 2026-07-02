/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JdbcSinkConnectorMetrics}.
 *
 * @author Lee Jaemin
 */
class JdbcSinkConnectorMetricsTest {

    @Test
    void shouldCountUpsertDeleteAndTruncateOperations() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");

        metrics.onUpsert();
        metrics.onUpsert();
        metrics.onDelete();
        metrics.onTruncate();
        metrics.onTruncate();
        metrics.onTruncate();

        assertThat(metrics.getTotalNumberOfUpsertEventsSeen()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfDeleteEventsSeen()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfTruncateEventsSeen()).isEqualTo(3);
    }

    @Test
    void shouldCountFilteredEvents() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");

        metrics.onFilteredEvent();
        metrics.onFilteredEvent();

        assertThat(metrics.getNumberOfFilteredEvents()).isEqualTo(2);
    }

    @Test
    void shouldToggleConnectedState() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");
        assertThat(metrics.isConnected()).isFalse();

        metrics.connected(true);
        assertThat(metrics.isConnected()).isTrue();

        metrics.connected(false);
        assertThat(metrics.isConnected()).isFalse();
    }

    @Test
    void shouldRegisterAndUnregisterMBeanUnderDebeziumJdbcDomain() throws Exception {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = new ObjectName("debezium.jdbc:type=connector-metrics,context=sink,server=my-sink,task=0");

        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");
        try {
            metrics.register();
            assertThat(server.isRegistered(objectName)).isTrue();

            metrics.onUpsert();
            assertThat(server.getAttribute(objectName, "TotalNumberOfUpsertEventsSeen")).isEqualTo(1L);
        }
        finally {
            metrics.unregister();
        }
        assertThat(server.isRegistered(objectName)).isFalse();
    }
}
