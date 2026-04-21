/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.source.kafka;

import java.util.Map;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Adapter that wraps Kafka Connect's {@link SourceTaskContext} to adapt it for Debezium's internal usage.
 *
 * @author Debezium Authors
 */
public class KafkaSourceTaskContextAdapter implements SourceTaskContext {
    private final SourceTaskContext delegate;

    public KafkaSourceTaskContextAdapter(SourceTaskContext delegate) {
        this.delegate = delegate;
    }

    public KafkaSourceTaskContextAdapter(final Map<String, String> config, final OffsetStorageReader offsetReader) {
        this.delegate = new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return config;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return offsetReader;
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null; // not supported
            }
        };
    }

    @Override
    public Map<String, String> configs() {
        return delegate.configs();
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return delegate.offsetStorageReader();
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return delegate.pluginMetrics();
    }

    /**
     * Returns the underlying Kafka SourceTaskContext.
     *
     * @return the wrapped SourceTaskContext
     */
    public SourceTaskContext getDelegate() {
        return delegate;
    }
}
