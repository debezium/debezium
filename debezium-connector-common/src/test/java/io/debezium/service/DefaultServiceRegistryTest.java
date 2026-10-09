/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.config.Configuration;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.service.spi.ServiceRegistry;

/**
 * Verifies that the {@link DefaultServiceRegistry} registers Debezium's default service providers.
 */
class DefaultServiceRegistryTest {

    @Test
    void shouldRegisterDefaultServiceProviders() {
        try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
            assertThat(registry.getService(CustomConverterRegistry.class)).isNotNull();
        }
    }
}
