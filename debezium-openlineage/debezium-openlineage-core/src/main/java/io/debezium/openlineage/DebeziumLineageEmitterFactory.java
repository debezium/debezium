/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.openlineage.dataset.DatasetNamespaceResolverFactory;
import io.debezium.openlineage.dataset.DefaultDatasetNamespaceResolverFactory;
import io.debezium.openlineage.emitter.DebeziumOpenLineageClient;
import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.LineageEmitterFactory;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEmitter;
import io.openlineage.client.OpenLineage;

/**
 * @author Mario Fiore Vitale
 */
public class DebeziumLineageEmitterFactory implements LineageEmitterFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumLineageEmitterFactory.class);
    private static final ServiceLoader<DatasetNamespaceResolverFactory> datasetNamespaceResolverFactory = ServiceLoader.load(DatasetNamespaceResolverFactory.class);
    private final AtomicReference<OpenLineageContext> contextRef = new AtomicReference<>();

    @Override
    public LineageEmitter get(ConnectorContext connectorContext) {

        DebeziumOpenLineageConfiguration debeziumOpenLineageConfiguration = DebeziumOpenLineageConfiguration.from(connectorContext);

        if (debeziumOpenLineageConfiguration.enabled()) {
            DebeziumOpenLineageClient emitter = new DebeziumOpenLineageClient(connectorContext, debeziumOpenLineageConfiguration);

            if (contextRef.get() == null) {
                LOGGER.debug("OpenLineageContext was null, getting instance");
                OpenLineageContext ctx = new OpenLineageContext(
                        new OpenLineage(emitter.getProducer()),
                        debeziumOpenLineageConfiguration,
                        OpenLineageJobIdentifier.from(connectorContext, debeziumOpenLineageConfiguration));
                contextRef.compareAndSet(null, ctx);
            }

            DatasetNamespaceResolverFactory namespaceResolverFactory = datasetNamespaceResolverFactory
                    .stream()
                    .findFirst()
                    .map(ServiceLoader.Provider::get)
                    .orElse(new DefaultDatasetNamespaceResolverFactory());

            LOGGER.debug("OpenLineageContext {}", contextRef.get());
            return new OpenLineageEmitter(connectorContext, contextRef.get(), emitter, namespaceResolverFactory);
        }

        return new NoOpLineageEmitter();
    }
}
