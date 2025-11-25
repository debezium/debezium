/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.connector.common.DebeziumHeaderProducerProvider;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistryBuilder;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterServiceProvider;

/**
 * Supplies all common, default service providers to Debezium's service registry.
 *
 * @author Chris Cranford
 */
public class DefaultServiceProviderContributor implements ServiceProviderContributor {
    @Override
    public void contribute(ServiceRegistryBuilder registryBuilder) {
        registryBuilder.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        registryBuilder.registerServiceProvider(new SnapshotLockProvider());
        registryBuilder.registerServiceProvider(new SnapshotQueryProvider());
        registryBuilder.registerServiceProvider(new SnapshotterServiceProvider());
        registryBuilder.registerServiceProvider(new DebeziumHeaderProducerProvider());
        registryBuilder.registerServiceProvider(new CustomConverterServiceProvider());
    }
}
