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
 */
class JdbcSinkConnectorMetricsTest {

    @Test
    void shouldCountInsertUpdateUpsertDeleteAndTruncateOperations() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");

        metrics.inserted();
        metrics.updated();
        metrics.updated();
        metrics.upserted();
        metrics.upserted();
        metrics.deleted();
        metrics.truncated();
        metrics.truncated();
        metrics.truncated();

        assertThat(metrics.getTotalNumberOfInsertEventsSeen()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfUpdateEventsSeen()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfUpsertEventsSeen()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfDeleteEventsSeen()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfTruncateEventsSeen()).isEqualTo(3);
    }

    @Test
    void shouldCountCreatedAndAlteredTables() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");

        metrics.tableCreated();
        metrics.tableAltered();
        metrics.tableAltered();

        assertThat(metrics.getTotalNumberOfTablesCreated()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfTablesAltered()).isEqualTo(2);
    }

    @Test
    void shouldCountFilteredEvents() {
        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");

        metrics.filtered();
        metrics.filtered();

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

            metrics.inserted();
            metrics.updated();
            metrics.upserted();
            assertThat(server.getAttribute(objectName, "TotalNumberOfInsertEventsSeen")).isEqualTo(1L);
            assertThat(server.getAttribute(objectName, "TotalNumberOfUpdateEventsSeen")).isEqualTo(1L);
            assertThat(server.getAttribute(objectName, "TotalNumberOfUpsertEventsSeen")).isEqualTo(1L);
        }
        finally {
            metrics.unregister();
        }
        assertThat(server.isRegistered(objectName)).isFalse();
    }
}
