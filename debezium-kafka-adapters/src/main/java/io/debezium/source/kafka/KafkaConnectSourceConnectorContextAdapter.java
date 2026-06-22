/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.source.kafka;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.debezium.util.KafkaConnectUtil;

/**
 *  Adapter that wraps Kafka Connect's {@link SourceConnectorContext} to adapt it for Debezium's internal usage.
 *
 * @author Debezium Authors
 */
public class KafkaConnectSourceConnectorContextAdapter {

    private final SourceConnectorContext delegate;

    public KafkaConnectSourceConnectorContextAdapter(SourceConnectorContext delegate) {
        this.delegate = delegate;
    }

    public KafkaConnectSourceConnectorContextAdapter(OffsetStorageReader reader) {
        this.delegate = createDelegate(reader);
    }

    public KafkaConnectSourceConnectorContextAdapter(io.debezium.spi.storage.OffsetStorageReader reader) {
        this.delegate = createDelegate(KafkaConnectUtil.toKafkaReader(reader));
    }

    private static SourceConnectorContext createDelegate(OffsetStorageReader reader) {
        return new SourceConnectorContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return reader;
            }

            @Override
            public void requestTaskReconfiguration() {
                // not supported
            }

            @Override
            public void raiseError(Exception e) {
                // not supported
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null; // not supported
            }
        };
    }

    /**
     * Returns the underlying Kafka SourceConnectorContext.
     *
     * @return the wrapped SourceConnectorContext
     */
    public SourceConnectorContext getDelegate() {
        return delegate;
    }
}
