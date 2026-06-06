/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.config.Configuration;

/**
 * Unit tests for the {@code binlog.net.write.timeout} and {@code binlog.net.read.timeout}
 * configuration properties introduced in mysql-binlog-connector-java.
 *
 * @author Jia Fan
 */
public abstract class BinlogNetTimeoutConfigTest<C extends SourceConnector> implements BinlogConnectorTest<C> {

    protected abstract BinlogConnectorConfig createConnectorConfig(Configuration config);

    @Test
    void shouldReturnDefaultNetWriteTimeout() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1234)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        final BinlogConnectorConfig connectorConfig = createConnectorConfig(config);
        assertThat(connectorConfig.getBinlogNetWriteTimeout()).isEqualTo(0L);
    }

    @Test
    void shouldReturnDefaultNetReadTimeout() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1234)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        final BinlogConnectorConfig connectorConfig = createConnectorConfig(config);
        assertThat(connectorConfig.getBinlogNetReadTimeout()).isEqualTo(0L);
    }

    @Test
    void shouldReturnConfiguredNetWriteTimeout() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1234)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_NET_WRITE_TIMEOUT, 120)
                .build();

        final BinlogConnectorConfig connectorConfig = createConnectorConfig(config);
        assertThat(connectorConfig.getBinlogNetWriteTimeout()).isEqualTo(120L);
    }

    @Test
    void shouldReturnConfiguredNetReadTimeout() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1234)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_NET_READ_TIMEOUT, 300)
                .build();

        final BinlogConnectorConfig connectorConfig = createConnectorConfig(config);
        assertThat(connectorConfig.getBinlogNetReadTimeout()).isEqualTo(300L);
    }

    @Test
    void shouldApplyNetWriteTimeoutToBinaryLogClient() {
        final BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
        client.setNetWriteTimeout(120);
        assertThat(client.getNetWriteTimeout()).isEqualTo(120L);
    }

    @Test
    void shouldApplyNetReadTimeoutToBinaryLogClient() {
        final BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
        client.setNetReadTimeout(300);
        assertThat(client.getNetReadTimeout()).isEqualTo(300L);
    }

    @Test
    void shouldNotApplyNetWriteTimeoutWhenZero() {
        final BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
        // Default should be 0 (not set)
        assertThat(client.getNetWriteTimeout()).isEqualTo(0L);
    }

    @Test
    void shouldNotApplyNetReadTimeoutWhenZero() {
        final BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
        // Default should be 0 (not set)
        assertThat(client.getNetReadTimeout()).isEqualTo(0L);
    }

    @Test
    void shouldReturnBothTimeoutsConfiguredTogether() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1234)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_NET_WRITE_TIMEOUT, 600)
                .with(BinlogConnectorConfig.BINLOG_NET_READ_TIMEOUT, 900)
                .build();

        final BinlogConnectorConfig connectorConfig = createConnectorConfig(config);
        assertThat(connectorConfig.getBinlogNetWriteTimeout()).isEqualTo(600L);
        assertThat(connectorConfig.getBinlogNetReadTimeout()).isEqualTo(900L);
    }
}
