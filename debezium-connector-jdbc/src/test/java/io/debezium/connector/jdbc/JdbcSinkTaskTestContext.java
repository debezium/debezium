/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * A simple mock {@link SinkTaskContext} for execution JDBC sink connector tests.
 *
 * @author Chris Cranford
 */
public class JdbcSinkTaskTestContext implements SinkTaskContext {

    private final Map<String, String> properties;

    public JdbcSinkTaskTestContext(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> configs() {
        return properties;
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void timeout(long l) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void requestCommit() {
        // no-op
    }

    @Override
    public PluginMetrics pluginMetrics() {
        // No metrics are exposed yet
        return null;
    }
}
