/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.config.Configuration;
import io.debezium.service.spi.InstanceResolver;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;

/**
 * Provides the {@link DefaultInstanceResolver} to Debezium's service registry.
 *
 * @author Chris Cranford
 */
public class InstanceResolverServiceProvider implements ServiceProvider<InstanceResolver> {

    @Override
    public Class<InstanceResolver> getServiceClass() {
        return InstanceResolver.class;
    }

    @Override
    public InstanceResolver createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        return new DefaultInstanceResolver();
    }
}
