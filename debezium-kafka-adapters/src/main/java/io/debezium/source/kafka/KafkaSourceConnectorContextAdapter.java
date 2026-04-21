/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.source.kafka;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 *  Adapter that wraps Kafka Connect's {@link SourceConnectorContext} to adapt it for Debezium's internal usage.
 *
 * @author Debezium Authors
 */
public class KafkaSourceConnectorContextAdapter implements SourceConnectorContext {

    private final SourceConnectorContext delegate;

    public KafkaSourceConnectorContextAdapter(SourceConnectorContext delegate) {
        this.delegate = delegate;
    }

    public KafkaSourceConnectorContextAdapter(OffsetStorageReader reader) {
        this.delegate = new SourceConnectorContext() {
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
                // not supporter
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null; // not supported
            }
        };
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return delegate.offsetStorageReader();
    }

    @Override
    public void requestTaskReconfiguration() {
        delegate.requestTaskReconfiguration();
    }

    @Override
    public void raiseError(Exception e) {
        delegate.raiseError(e);
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return delegate.pluginMetrics();
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
