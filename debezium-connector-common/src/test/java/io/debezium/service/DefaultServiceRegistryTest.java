/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.config.Configuration;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryBuilder;

/**
 * Verifies that the {@link DefaultServiceRegistry} registers Debezium's default service providers and
 * that a {@link ServiceProviderContributor} found by the {@link java.util.ServiceLoader} replaces them.
 */
class DefaultServiceRegistryTest {

    private static final CustomConverterRegistry CONTRIBUTED_REGISTRY = new CustomConverterRegistry(List.of());

    @Test
    void shouldRegisterDefaultServiceProviders() {
        try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
            assertThat(registry.getService(CustomConverterRegistry.class)).isNotNull().isNotSameAs(CONTRIBUTED_REGISTRY);
        }
    }

    @Test
    void shouldReplaceDefaultServiceProviderWithContributedProvider(@TempDir Path classpathRoot) throws Exception {
        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
                assertThat(registry.getService(CustomConverterRegistry.class)).isSameAs(CONTRIBUTED_REGISTRY);
            }
        });
    }

    public static class TestContributor implements ServiceProviderContributor {
        @Override
        public void contribute(ServiceRegistryBuilder registryBuilder) {
            registryBuilder.registerServiceProvider(new ServiceProvider<CustomConverterRegistry>() {
                @Override
                public CustomConverterRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
                    return CONTRIBUTED_REGISTRY;
                }

                @Override
                public Class<CustomConverterRegistry> getServiceClass() {
                    return CustomConverterRegistry.class;
                }
            });
        }
    }
}
