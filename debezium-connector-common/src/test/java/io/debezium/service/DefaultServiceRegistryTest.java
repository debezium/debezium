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
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.service.spi.InstanceResolver;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceProviderRegistrar;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotterService;

/**
 * Verifies that the {@link DefaultServiceRegistry} registers only the {@link InstanceResolver} provider
 * itself and that a provider supplied by a {@link ServiceProviderContributor} found by the
 * {@link java.util.ServiceLoader} takes precedence over the defaults registered by a connector task.
 */
class DefaultServiceRegistryTest {

    private static final CustomConverterRegistry CONTRIBUTED_REGISTRY = new CustomConverterRegistry(List.of());

    @Test
    void shouldRegisterInstanceResolverProvider() {
        try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
            assertThat(registry.getService(InstanceResolver.class)).isNotNull();
        }
    }

    @Test
    void shouldNotRegisterProvidersThatConnectorTasksRegister() {
        // Connectors without snapshot support, such as Vitess and Spanner, omit the snapshotter provider
        // from their task's registrations and rely on the service being absent.
        try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
            assertThat(registry.tryGetService(SnapshotterService.class)).isNull();
        }
    }

    @Test
    void shouldUseProviderRegisteredByConnectorTask() {
        try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
            registry.registerServiceProvider(new CustomConverterServiceProvider());
            assertThat(registry.getService(CustomConverterRegistry.class)).isNotNull().isNotSameAs(CONTRIBUTED_REGISTRY);
        }
    }

    @Test
    void shouldKeepContributedProviderWhenConnectorTaskRegistersDefault(@TempDir Path classpathRoot) throws Exception {
        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            try (ServiceRegistry registry = new DefaultServiceRegistry(Configuration.empty(), new DefaultBeanRegistry())) {
                registry.registerServiceProvider(new CustomConverterServiceProvider());
                assertThat(registry.getService(CustomConverterRegistry.class)).isSameAs(CONTRIBUTED_REGISTRY);
            }
        });
    }

    public static class TestContributor implements ServiceProviderContributor {
        @Override
        public void contribute(ServiceProviderRegistrar registrar) {
            registrar.registerServiceProvider(new ServiceProvider<CustomConverterRegistry>() {
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
