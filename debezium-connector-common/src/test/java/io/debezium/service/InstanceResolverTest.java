/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.heartbeat.DebeziumHeartbeatFactory;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.junit.relational.TestHistorizedRelationalDatabaseConfig;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.processors.PostProcessorRegistry;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.service.spi.InstanceResolver;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryBuilder;
import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Verifies the behavior of the {@link DefaultInstanceResolver} and that the topic naming strategy, the
 * post processors and the custom converters are resolved through the {@link InstanceResolver} that is
 * registered with the service registry.
 */
class InstanceResolverTest {

    private static final SuppliedTopicNamingStrategy SUPPLIED_STRATEGY = new SuppliedTopicNamingStrategy();
    private static final PostProcessor SUPPLIED_POST_PROCESSOR = new TestPostProcessor();
    private static final CustomConverter<SchemaBuilder, ConvertedField> SUPPLIED_CUSTOM_CONVERTER = new TestCustomConverter();
    private static final TestHeartbeatFactory SUPPLIED_HEARTBEAT_FACTORY = new TestHeartbeatFactory();
    private static final SchemaHistory SUPPLIED_SCHEMA_HISTORY = new MemorySchemaHistory();
    private static final TestSourceInfoStructMaker SUPPLIED_SOURCE_INFO_STRUCT_MAKER = new TestSourceInfoStructMaker();

    private static final TableId TABLE = new TableId("db", null, "t");
    private static final Column COLUMN = Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create();

    private final Configuration config = Configuration.create()
            .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
            .build();

    private final Configuration heartbeatConfig = Configuration.create()
            .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
            .with(Heartbeat.HEARTBEAT_INTERVAL, 1000)
            .build();

    private final Configuration schemaHistoryConfig = Configuration.create()
            .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
            .with(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName())
            .build();

    private final Configuration sourceInfoStructMakerConfig = Configuration.create()
            .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
            .with(CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER, TestSourceInfoStructMaker.class.getName())
            .build();

    private final Configuration postProcessorConfig = Configuration.create()
            .with(CommonConnectorConfig.CUSTOM_POST_PROCESSORS, "test")
            .with(CommonConnectorConfig.CUSTOM_POST_PROCESSORS.name() + ".test.type", TestPostProcessor.class.getName())
            .build();

    private final Configuration customConverterConfig = Configuration.create()
            .with(CommonConnectorConfig.CUSTOM_CONVERTERS, "test")
            .with("test" + CustomConverterServiceProvider.CONVERTER_TYPE_SUFFIX, TestCustomConverter.class.getName())
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

    @Test
    void shouldCreatePostProcessorsFromConfigurationByDefault() {
        try (ServiceRegistry registry = new DefaultServiceRegistry(postProcessorConfig, new DefaultBeanRegistry())) {
            assertThat(registry.getService(PostProcessorRegistry.class).getProcessors())
                    .singleElement()
                    .isExactlyInstanceOf(TestPostProcessor.class);
        }
    }

    @Test
    void shouldAddPostProcessorsSuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            try (ServiceRegistry registry = new DefaultServiceRegistry(postProcessorConfig, new DefaultBeanRegistry())) {
                final List<PostProcessor> processors = registry.getService(PostProcessorRegistry.class).getProcessors();

                assertThat(processors).hasSize(2);
                assertThat(processors.get(0)).isExactlyInstanceOf(TestPostProcessor.class).isNotSameAs(SUPPLIED_POST_PROCESSOR);
                assertThat(processors.get(1)).isSameAs(SUPPLIED_POST_PROCESSOR);
            }
        });
    }

    @Test
    void shouldCreateCustomConvertersFromConfigurationByDefault() {
        TestCustomConverter.CONSULTED.clear();

        try (ServiceRegistry registry = new DefaultServiceRegistry(customConverterConfig, new DefaultBeanRegistry())) {
            registry.getService(CustomConverterRegistry.class).registerConverterFor(TABLE, COLUMN, null);

            assertThat(TestCustomConverter.CONSULTED)
                    .singleElement()
                    .isExactlyInstanceOf(TestCustomConverter.class)
                    .isNotSameAs(SUPPLIED_CUSTOM_CONVERTER);
        }
    }

    @Test
    void shouldAddCustomConvertersSuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        TestCustomConverter.CONSULTED.clear();

        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            try (ServiceRegistry registry = new DefaultServiceRegistry(customConverterConfig, new DefaultBeanRegistry())) {
                registry.getService(CustomConverterRegistry.class).registerConverterFor(TABLE, COLUMN, null);

                assertThat(TestCustomConverter.CONSULTED).hasSize(2);
                assertThat(TestCustomConverter.CONSULTED.get(0)).isExactlyInstanceOf(TestCustomConverter.class).isNotSameAs(SUPPLIED_CUSTOM_CONVERTER);
                assertThat(TestCustomConverter.CONSULTED.get(1)).isSameAs(SUPPLIED_CUSTOM_CONVERTER);
            }
        });
    }

    @Test
    void shouldCreateHeartbeatFromDefaultFactoriesByDefault() {
        SUPPLIED_HEARTBEAT_FACTORY.connectorConfig = null;
        final var connectorConfig = new TestRelationalDatabaseConfig(heartbeatConfig, null, null, 0);

        final var heartbeat = new HeartbeatFactory<>().getScheduledHeartbeat(connectorConfig, null, null, queue());

        assertThat(heartbeat).isNotSameAs(Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT);
        assertThat(SUPPLIED_HEARTBEAT_FACTORY.connectorConfig).isNull();
    }

    @Test
    void shouldAddHeartbeatFactoriesSuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        SUPPLIED_HEARTBEAT_FACTORY.connectorConfig = null;

        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            final var connectorConfig = new TestRelationalDatabaseConfig(heartbeatConfig, null, null, 0);

            final var heartbeat = new HeartbeatFactory<>().getScheduledHeartbeat(connectorConfig, null, null, queue());

            assertThat(heartbeat).isNotSameAs(Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT);
            assertThat(SUPPLIED_HEARTBEAT_FACTORY.connectorConfig).isSameAs(connectorConfig);
        });
    }

    @Test
    void shouldCreateSchemaHistoryFromConfigurationByDefault() {
        final var connectorConfig = new TestHistorizedRelationalDatabaseConfig(schemaHistoryConfig);

        assertThat(connectorConfig.getSchemaHistory())
                .isExactlyInstanceOf(MemorySchemaHistory.class)
                .isNotSameAs(SUPPLIED_SCHEMA_HISTORY);
    }

    @Test
    void shouldUseSchemaHistorySuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            final var connectorConfig = new TestHistorizedRelationalDatabaseConfig(schemaHistoryConfig);

            assertThat(connectorConfig.getSchemaHistory()).isSameAs(SUPPLIED_SCHEMA_HISTORY);
        });
    }

    @Test
    void shouldCreateSourceInfoStructMakerFromConfigurationByDefault() {
        SUPPLIED_SOURCE_INFO_STRUCT_MAKER.connector = null;

        final var connectorConfig = sourceInfoStructMakerConfig(sourceInfoStructMakerConfig);

        assertThat(connectorConfig.getSourceInfoStructMaker())
                .isExactlyInstanceOf(TestSourceInfoStructMaker.class)
                .isNotSameAs(SUPPLIED_SOURCE_INFO_STRUCT_MAKER);
        assertThat(SUPPLIED_SOURCE_INFO_STRUCT_MAKER.connector).isNull();
    }

    @Test
    void shouldUseSourceInfoStructMakerSuppliedByContributedResolver(@TempDir Path classpathRoot) throws Exception {
        SUPPLIED_SOURCE_INFO_STRUCT_MAKER.connector = null;

        TestContributors.runWith(classpathRoot, TestContributor.class, () -> {
            final var connectorConfig = sourceInfoStructMakerConfig(sourceInfoStructMakerConfig);

            assertThat(connectorConfig.getSourceInfoStructMaker()).isSameAs(SUPPLIED_SOURCE_INFO_STRUCT_MAKER);
            assertThat(SUPPLIED_SOURCE_INFO_STRUCT_MAKER.connector).isEqualTo("test");
        });
    }

    /**
     * Creates a connector configuration that resolves its source info struct maker from the configuration,
     * as the connectors do, rather than the test default of none.
     */
    private static CommonConnectorConfig sourceInfoStructMakerConfig(Configuration config) {
        return new TestRelationalDatabaseConfig(config, null, null, 0) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return getSourceInfoStructMaker(CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER, "test", "1.0", this);
            }
        };
    }

    private static ChangeEventQueue<DataChangeEvent> queue() {
        return new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(100))
                .maxBatchSize(10)
                .maxQueueSize(100)
                .build();
    }

    /**
     * Contributes a resolver that supplies its own topic naming strategy, post processor, custom converter and
     * heartbeat factory, like a runtime environment would.
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
            if (contract == TopicNamingStrategy.class) {
                final T instance = contract.cast(SUPPLIED_STRATEGY);
                SUPPLIED_STRATEGY.configKey = configKey;
                initializer.accept(instance);
                return instance;
            }
            if (contract == SchemaHistory.class) {
                return contract.cast(SUPPLIED_SCHEMA_HISTORY);
            }
            if (contract == SourceInfoStructMaker.class) {
                return contract.cast(SUPPLIED_SOURCE_INFO_STRUCT_MAKER);
            }
            return fallback.get();
        }

        @Override
        public <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback, Consumer<? super T> initializer) {
            final List<T> instances = new ArrayList<>(fallback.get());
            if (contract == PostProcessor.class) {
                instances.add(contract.cast(SUPPLIED_POST_PROCESSOR));
            }
            if (contract == CustomConverter.class) {
                instances.add(contract.cast(SUPPLIED_CUSTOM_CONVERTER));
            }
            if (contract == DebeziumHeartbeatFactory.class) {
                instances.add(contract.cast(SUPPLIED_HEARTBEAT_FACTORY));
            }
            return instances;
        }
    }

    private static class TestHeartbeatFactory implements DebeziumHeartbeatFactory {

        private CommonConnectorConfig connectorConfig;

        @Override
        public Optional<Heartbeat> getHeartbeat(CommonConnectorConfig connectorConfig, HeartbeatConnectionProvider connectionProvider,
                                                HeartbeatErrorHandler errorHandler, ChangeEventQueue<DataChangeEvent> queue) {
            this.connectorConfig = connectorConfig;
            return Optional.of(new Heartbeat() {
                @Override
                public void emit(Map<String, ?> partition, OffsetContext offset) {
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            });
        }
    }

    public static class TestSourceInfoStructMaker implements SourceInfoStructMaker<AbstractSourceInfo> {

        private String connector;

        @Override
        public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
            this.connector = connector;
        }

        @Override
        public Schema schema() {
            return SchemaBuilder.struct().build();
        }

        @Override
        public Struct struct(AbstractSourceInfo sourceInfo) {
            return null;
        }
    }

    public static class TestCustomConverter implements CustomConverter<SchemaBuilder, ConvertedField> {

        static final List<CustomConverter<SchemaBuilder, ConvertedField>> CONSULTED = new ArrayList<>();

        @Override
        public void configure(Properties props) {
        }

        @Override
        public void converterFor(ConvertedField field, ConverterRegistration<SchemaBuilder> registration) {
            CONSULTED.add(this);
        }
    }

    public static class TestPostProcessor implements PostProcessor {
        @Override
        public void configure(Map<String, ?> properties) {
        }

        @Override
        public void apply(Object key, Struct value) {
        }

        @Override
        public void close() {
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
