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

        metrics.written(1);
        metrics.deleted(1);
        metrics.truncated();
        metrics.truncated();
        metrics.truncated();

        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfDeletes()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfTruncates()).isEqualTo(3);
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

        assertThat(metrics.getTotalNumberOfFilteredEvents()).isEqualTo(2);
    }

    @Test
    void shouldRegisterAndUnregisterMBeanUnderDebeziumJdbcDomain() throws Exception {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = new ObjectName("debezium.jdbc:type=connector-metrics,context=sink,server=my-sink,task=0");

        final JdbcSinkConnectorMetrics metrics = new JdbcSinkConnectorMetrics("my-sink", "0");
        try {
            metrics.register();
            assertThat(server.isRegistered(objectName)).isTrue();

            metrics.written(1);
            assertThat(server.getAttribute(objectName, "TotalNumberOfWrites")).isEqualTo(1L);
        }
        finally {
            metrics.unregister();
        }
        assertThat(server.isRegistered(objectName)).isFalse();
    }
}
