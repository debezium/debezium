/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.service.spi.InstanceResolver;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Verifies the behavior of the {@link DefaultInstanceResolver} and that the topic naming strategy is
 * resolved through the {@link InstanceResolver} that is registered with the service registry.
 */
class InstanceResolverTest {

    private static final SuppliedTopicNamingStrategy SUPPLIED_STRATEGY = new SuppliedTopicNamingStrategy();

    private final Configuration config = Configuration.create()
            .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
            .build();

    @Test
    void shouldResolveInstanceFromFallbackByDefault() {
        final var instance = new Object();

        assertThat(new DefaultInstanceResolver().resolve(Object.class, "some.key", () -> instance)).isSameAs(instance);
    }

    @Test
    void shouldNotApplyInitializerToFallbackInstanceByDefault() {
        final var initialized = new AtomicBoolean();

        new DefaultInstanceResolver().resolve(Object.class, "some.key", Object::new, instance -> initialized.set(true));

        assertThat(initialized).isFalse();
    }

    @Test
    void shouldCreateTopicNamingStrategyFromConfigurationByDefault() {
        final var connectorConfig = new TestRelationalDatabaseConfig(config, null, null, 0);

        assertThat(connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY))
                .isExactlyInstanceOf(SchemaTopicNamingStrategy.class);
    }

    @Test
    void shouldUseTopicNamingStrategySuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            final var connectorConfig = new TestRelationalDatabaseConfig(config, null, null, 0);

            assertThat(connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY)).isSameAs(SUPPLIED_STRATEGY);
            assertThat(SUPPLIED_STRATEGY.configKey).isEqualTo(CommonConnectorConfig.TOPIC_NAMING_STRATEGY.name());
            assertThat(SUPPLIED_STRATEGY.props).containsEntry(CommonConnectorConfig.TOPIC_PREFIX.name(), "server1");
        });
    }

    @Test
    void shouldResolveAllInstancesFromFallbackByDefault() {
        final var initialized = new AtomicBoolean();
        final List<String> fallback = List.of("one", "two");

        final List<String> instances = new DefaultInstanceResolver().resolveAll(String.class, () -> fallback, instance -> initialized.set(true));

        assertThat(instances).containsExactly("one", "two").isNotSameAs(fallback);
        assertThat(initialized).isFalse();
    }

    /**
     * Contributes a resolver that supplies its own topic naming strategy, like a runtime environment would.
     */
    public static class TestContributor implements ServiceProviderContributor {
        @Override
        public void contribute(ServiceRegistryBuilder registryBuilder) {
            registryBuilder.registerServiceProvider(new ServiceProvider<InstanceResolver>() {
                @Override
                public Class<InstanceResolver> getServiceClass() {
                    return InstanceResolver.class;
                }

                @Override
                public InstanceResolver createService(Configuration configuration, ServiceRegistry serviceRegistry) {
                    return new SupplyingInstanceResolver();
                }
            });
        }
    }

    private static class SupplyingInstanceResolver implements InstanceResolver {
        @Override
        public <T> T resolve(Class<T> contract, String configKey, Supplier<? extends T> fallback, Consumer<? super T> initializer) {
            if (contract != TopicNamingStrategy.class) {
                return fallback.get();
            }
            final T instance = contract.cast(SUPPLIED_STRATEGY);
            SUPPLIED_STRATEGY.configKey = configKey;
            initializer.accept(instance);
            return instance;
        }

        @Override
        public <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback, Consumer<? super T> initializer) {
            return new ArrayList<>(fallback.get());
        }
    }

    private static class SuppliedTopicNamingStrategy implements TopicNamingStrategy<TableId> {

        private String configKey;
        private Properties props;

        @Override
        public void configure(Properties props) {
            this.props = props;
        }

        @Override
        public String dataChangeTopic(TableId id) {
            return id.identifier();
        }

        @Override
        public String schemaChangeTopic() {
            return "schema";
        }

        @Override
        public String heartbeatTopic() {
            return "heartbeat";
        }

        @Override
        public String transactionTopic() {
            return "transaction";
        }

        @Override
        public String sanitizedTopicName(String topicName) {
            return topicName;
        }
    }
}
