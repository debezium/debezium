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

import io.debezium.config.Configuration;
import io.debezium.openlineage.dataset.DatasetNamespaceResolverFactory;
import io.debezium.openlineage.dataset.DefaultDatasetNamespaceResolverFactory;
import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.LineageEmitterFactory;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEventEmitter;
import io.openlineage.client.OpenLineage;

public class DebeziumLineageEmitterFactory implements LineageEmitterFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumLineageEmitterFactory.class);
    private static final String CONNECTOR_NAME_PROPERTY = "name";
    private static final ServiceLoader<DatasetNamespaceResolverFactory> datasetNamespaceResolverFactory = ServiceLoader.load(DatasetNamespaceResolverFactory.class);
    private final AtomicReference<OpenLineageContext> contextRef = new AtomicReference<>();

    @Override
    public LineageEmitter get(Configuration connectorConfig, String connName) {

        DebeziumOpenLineageConfiguration debeziumOpenLineageConfiguration = DebeziumOpenLineageConfiguration.from(connectorConfig);

        if (debeziumOpenLineageConfiguration.enabled()) {
            OpenLineageEventEmitter emitter = new OpenLineageEventEmitter(debeziumOpenLineageConfiguration);

            if (contextRef.get() == null) {
                LOGGER.debug("OpenLineageContext was null, getting instance");
                OpenLineageContext ctx = new OpenLineageContext(
                        new OpenLineage(emitter.getProducer()),
                        debeziumOpenLineageConfiguration,
                        new OpenLineageJobIdentifier(debeziumOpenLineageConfiguration.job().namespace(), connectorConfig.getString(CONNECTOR_NAME_PROPERTY)));
                contextRef.compareAndSet(null, ctx);
            }

            DatasetNamespaceResolverFactory namespaceResolverFactory = datasetNamespaceResolverFactory
                    .stream()
                    .findFirst()
                    .map(ServiceLoader.Provider::get)
                    .orElse(new DefaultDatasetNamespaceResolverFactory());

            LOGGER.debug("OpenLineageContext {}", contextRef.get());
            return new OpenLineageEmitter(connName, connectorConfig, contextRef.get(), emitter, namespaceResolverFactory.createInput(connName),
                    namespaceResolverFactory.createOutput(connName));
        }

        return new NoOpLineageEmitter();
    }
}
