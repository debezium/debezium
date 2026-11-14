/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class MongoSinkTaskTestContext implements SinkTaskContext {

    private final Map<String, String> properties;

    public MongoSinkTaskTestContext(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> configs() {
        return properties;
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
    }

    @Override
    public void timeout(long l) {
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Set.of();
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
    }

    @Override
    public void requestCommit() {
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return null;
    }
}
