/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.source.kafka.KafkaConnectSourceConnectorContextAdapter;
import io.debezium.source.kafka.KafkaConnectSourceTaskContextAdapter;

/**
 * Implementation of {@link DebeziumSourceConnector} which currently serves only as a wrapper
 * around Kafka Connect {@link SourceConnector}.
 *
 * @author vjuranek
 */
public class EngineSourceConnector implements DebeziumSourceConnector {

    private final SourceConnector connectConnector;
    private DebeziumSourceConnectorContext context;

    public EngineSourceConnector(final SourceConnector connectConnector) {
        this.connectConnector = connectConnector;
    }

    public SourceConnector connectConnector() {
        return connectConnector;
    }

    @Override
    public DebeziumSourceConnectorContext context() {
        return this.context;
    }

    @Override
    public void initialize(DebeziumSourceConnectorContext context) {
        this.context = context;
        // TODO: remove switching to Kafka
        this.connectConnector.initialize(new KafkaConnectSourceConnectorContextAdapter(context.offsetStorageReader()).getDelegate());
    }

    @Override
    public void start(Map<String, String> config) {
        connectConnector.start(config);
    }

    @Override
    public void stop() {
        connectConnector.stop();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return connectConnector.taskConfigs(maxTasks);
    }

    @Override
    public DebeziumSourceTask createTask(DebeziumSourceTaskContext context) throws Exception {
        final SourceTask task = (SourceTask) connectConnector.taskClass().getDeclaredConstructor().newInstance();
        // TODO: remove switching to Kafka
        task.initialize(new KafkaConnectSourceTaskContextAdapter(context.config(), context.offsetStorageReader()).getDelegate());
        return new EngineSourceTask(task, context);
    }
}
