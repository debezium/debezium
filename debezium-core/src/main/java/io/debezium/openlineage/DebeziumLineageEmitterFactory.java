/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.config.Configuration;
import io.debezium.openlineage.dataset.DatasetNamespaceResolverFactory;
import io.openlineage.client.OpenLineage;

public class DebeziumLineageEmitterFactory implements LineageEmitterFactory {

    private static final String CONNECTOR_NAME_PROPERTY = "name";
    private static final AtomicReference<OpenLineageContext> contextRef = new AtomicReference<>();

    @Override
    public LineageEmitter get(Configuration connectorConfig, String connName) {

        DebeziumOpenLineageConfiguration debeziumOpenLineageConfiguration = DebeziumOpenLineageConfiguration.from(connectorConfig);

        if (debeziumOpenLineageConfiguration.enabled()) {
            OpenLineageEventEmitter emitter = new OpenLineageEventEmitter(debeziumOpenLineageConfiguration);

            if (contextRef.get() == null) {
                OpenLineageContext ctx = new OpenLineageContext(
                        new OpenLineage(emitter.getProducer()),
                        debeziumOpenLineageConfiguration,
                        new OpenLineageJobIdentifier(debeziumOpenLineageConfiguration.job().namespace(), connectorConfig.getString(CONNECTOR_NAME_PROPERTY)));
                contextRef.compareAndSet(null, ctx);
            }

            return new OpenLineageEmitter(connName, connectorConfig, contextRef.get(), emitter, DatasetNamespaceResolverFactory.create(connName));
        }

        return new NoOpLineageEmitter();
    }
}
