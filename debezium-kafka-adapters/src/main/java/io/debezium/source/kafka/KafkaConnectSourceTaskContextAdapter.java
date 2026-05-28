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

import io.debezium.util.KafkaConnectUtil;

/**
 * Adapter that wraps Kafka Connect's {@link SourceTaskContext} to adapt it for Debezium's internal usage.
 *
 * @author Debezium Authors
 */
public class KafkaConnectSourceTaskContextAdapter {
    private final SourceTaskContext delegate;

    public KafkaConnectSourceTaskContextAdapter(SourceTaskContext delegate) {
        this.delegate = delegate;
    }

    public KafkaConnectSourceTaskContextAdapter(final Map<String, String> config, final OffsetStorageReader offsetReader) {
        this.delegate = createDelegate(config, offsetReader);
    }

    public KafkaConnectSourceTaskContextAdapter(final Map<String, String> config, final io.debezium.spi.storage.OffsetStorageReader offsetReader) {
        this.delegate = createDelegate(config, KafkaConnectUtil.toKafkaReader(offsetReader));
    }

    private static SourceTaskContext createDelegate(final Map<String, String> config, final OffsetStorageReader offsetReader) {
        return new SourceTaskContext() {
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

    /**
     * Returns the underlying Kafka SourceTaskContext.
     *
     * @return the wrapped SourceTaskContext
     */
    public SourceTaskContext getDelegate() {
        return delegate;
    }
}
