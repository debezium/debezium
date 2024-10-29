/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.PostgresExtensionUtils;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.WithPostgresExtension;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSourceNoMatrix;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipColumnTypePropagation;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipExtractNewRecordState;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSources;
import io.debezium.connector.jdbc.junit.jupiter.e2e.WithTemporalPrecisionMode;
import io.debezium.connector.jdbc.util.RandomTableNameGenerator;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.DebeziumKafkaContainer;

/**
 * The JDBC sink end to end pipeline context provider.
 *
 * This extension is responsible for creating the {@link Source} object and starting the Kafka
 * environment as well as the source databases needed for the test matrix. In addition, it is
 * also responsible for creating the permutations of the test template method invocations.
 *
 * @author Chris Cranford
 */
public class SourcePipelineInvocationContextProvider implements BeforeAllCallback, AfterAllCallback, TestTemplateInvocationContextProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourcePipelineInvocationContextProvider.class);

    private static final String MYSQL_IMAGE_NAME = "quay.io/debezium/example-mysql";
    private static final String MYSQL_USERNAME = "mysqluser";
    private static final String MYSQL_PASSWORD = "debezium";

    private static final String POSTGRES_IMAGE_NAME = "quay.io/debezium/example-postgres";
    private static final String POSTGRES_USERNAME = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";

    private static final String SQLSERVER_IMAGE_NAME = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_PASSWORD = "Debezium1!";

    private static final String ORACLE_IMAGE_NAME = "quay.io/rh_integration/dbz-oracle:19.3.0";
    private static final String ORACLE_USERNAME = "debezium";
    private static final String ORACLE_PASSWORD = "dbz";

    private static final Network network = Network.newNetwork();

    private final RandomTableNameGenerator tableNameGenerator;
    private final KafkaContainer kafkaContainer;
    private final DebeziumContainer connectContainer;
    private final Map<SourceType, JdbcDatabaseContainer<?>> sourceContainers;

    public SourcePipelineInvocationContextProvider() {
        this.tableNameGenerator = new RandomTableNameGenerator();
        this.kafkaContainer = getKafkaContainer();
        this.connectContainer = getKafkaConnectContainer(this.kafkaContainer);
        this.sourceContainers = getSourceContainers();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Create a stream of all containers to be started
        final Stream.Builder<Startable> startables = Stream.builder();
        startables.add(this.kafkaContainer);
        startables.add(this.connectContainer);
        sourceContainers.values().forEach(startables::add);

        // Start each container concurrently
        Startables.deepStart(startables.build()).join();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Stop containers
        this.sourceContainers.values().forEach(GenericContainer::stop);
        this.connectContainer.stop();
        this.kafkaContainer.stop();
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        final Method method = context.getRequiredTestMethod();

        final SkipWhenSource skipWhenSource = method.getAnnotation(SkipWhenSource.class);
        final SkipWhenSources skipWhenSources = method.getAnnotation(SkipWhenSources.class);
        final ForSource forSource = method.getAnnotation(ForSource.class);
        final ForSourceNoMatrix forSourceNoMatrix = method.getAnnotation(ForSourceNoMatrix.class);

        final WithPostgresExtension postgresExtensionAnn = method.getAnnotation(WithPostgresExtension.class);
        final String postgresExtension = Objects.isNull(postgresExtensionAnn) ? null : postgresExtensionAnn.value();

        boolean streamEmpty = true;
        final Stream.Builder<TestTemplateInvocationContext> builder = Stream.builder();
        for (SourceType sourceType : sourceContainers.keySet()) {
            if (isSkipped(skipWhenSource, skipWhenSources, sourceType)) {
                LOGGER.info("Skipped source connector {}, @SkipWhenSource detected.", sourceType);
                continue;
            }
            if (forSource != null && !Arrays.asList(forSource.value()).contains(sourceType)) {
                LOGGER.info("Skipped source connector {}, @ForSource does not include it.", sourceType);
                continue;
            }
            if (forSourceNoMatrix != null && !Arrays.asList(forSourceNoMatrix.value()).contains(sourceType)) {
                LOGGER.info("Skipped source connector {}, @ForSourceNoMatrix does not include it.", sourceType);
                continue;
            }

            for (Boolean flatten : getExtractNewRecordStateValues(method)) {
                for (Boolean propagateColumnTypes : getPropagateColumnTypeValues(method)) {
                    for (TemporalPrecisionMode temporalPrecisionMode : getTemporalPrecisionModes(method, sourceType)) {
                        builder.add(createInvocationContext(context, sourceType, flatten, propagateColumnTypes, temporalPrecisionMode, postgresExtension));
                        streamEmpty = false;
                    }
                }
            }
        }

        // Junit does not permit returning an empty invocation context list, which can happen if the test is
        // configured to only execute for a subset of sources and none of those sources are enabled via the
        // default source configuration or the environment variable "source.connectors".
        //
        // In this case to be compatible with Junit, we must return at least a single invocation context.
        // We return a single context here that marks the invocation as skipped due to no sources defined
        // that the test requires.
        if (streamEmpty) {
            builder.add(new TestTemplateInvocationContext() {
                @Override
                public String getDisplayName(int invocationIndex) {
                    return "skip-no-sources-available";
                }

                @Override
                public List<Extension> getAdditionalExtensions() {
                    return List.of((ExecutionCondition) context1 -> ConditionEvaluationResult.disabled("No sources available"));
                }
            });
        }

        return builder.build();
    }

    private boolean isSkipped(SkipWhenSource skipWhenSource, SkipWhenSources skipWhenSources, SourceType sourceType) {
        if (skipWhenSources != null) {
            for (SkipWhenSource skipWhenSourceChild : skipWhenSources.value()) {
                if (isSkipped(skipWhenSourceChild, null, sourceType)) {
                    return true;
                }
            }
        }
        if (skipWhenSource != null) {
            return Arrays.asList(skipWhenSource.value()).contains(sourceType);
        }
        return false;
    }

    private List<Boolean> getExtractNewRecordStateValues(Method method) {
        if (isAnyAnnotationPresent(method.getDeclaringClass(), SkipExtractNewRecordState.class) ||
                isAnyAnnotationPresent(method, SkipExtractNewRecordState.class, ForSourceNoMatrix.class)) {
            return List.of(false);
        }
        return List.of(false, true);
    }

    private List<Boolean> getPropagateColumnTypeValues(Method method) {
        if (isAnyAnnotationPresent(method.getDeclaringClass(), SkipColumnTypePropagation.class) ||
                isAnyAnnotationPresent(method, SkipColumnTypePropagation.class, ForSourceNoMatrix.class)) {
            return List.of(false);
        }
        return List.of(false, true);
    }

    private List<TemporalPrecisionMode> getTemporalPrecisionModes(Method method, SourceType sourceType) {
        if (isAnyAnnotationPresent(method, WithTemporalPrecisionMode.class)) {
            final List<TemporalPrecisionMode> result = new ArrayList<>();
            for (TemporalPrecisionMode temporalPrecisionMode : TemporalPrecisionMode.values()) {
                if (TemporalPrecisionMode.ADAPTIVE == temporalPrecisionMode && SourceType.MYSQL == sourceType) {
                    // MySQL explicitly prohibits the use of adaptive so we only allow the other two in the matrix.
                    continue;
                }
                result.add(temporalPrecisionMode);
            }
            return result;
        }
        return Collections.singletonList(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
    }

    @SafeVarargs
    private boolean isAnyAnnotationPresent(AnnotatedElement element, Class<? extends Annotation>... annotations) {
        for (Class<? extends Annotation> annotation : annotations) {
            if (element.isAnnotationPresent(annotation)) {
                return true;
            }
        }
        return false;
    }

    private TestTemplateInvocationContext createInvocationContext(ExtensionContext context, SourceType sourceType, boolean flatten,
                                                                  boolean propagateTypes, TemporalPrecisionMode temporalPrecisionMode,
                                                                  String postgresExtension) {
        final SourceConnectorOptions sourceOptions = new SourceConnectorOptions() {
            @Override
            public boolean useSnapshot() {
                return TestHelper.isSourceSnapshot();
            }

            @Override
            public boolean useDefaultValues() {
                return TestHelper.isDefaultValuesEnabled();
            }

            @Override
            public boolean isFlatten() {
                return flatten;
            }

            @Override
            public boolean isColumnTypePropagated() {
                return propagateTypes;
            }

            @Override
            public TemporalPrecisionMode getTemporalPrecisionMode() {
                return temporalPrecisionMode;
            }
        };

        final JdbcDatabaseContainer<?> sourceContainer = sourceContainers.get(sourceType);
        final Source source = new Source(sourceType, sourceContainer, kafkaContainer, connectContainer, sourceOptions, tableNameGenerator);

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "source-" + sourceType + "-[flat=" + flatten + ",propagate=" + propagateTypes + ",temporal=" + temporalPrecisionMode + "]";
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                final Method method = context.getRequiredTestMethod();
                final Class testClass = context.getRequiredTestClass();
                return Arrays.asList(
                        (BeforeEachCallback) context -> {
                            LOGGER.info("Running test {}.{}: {}", testClass.getName(), method.getName(), getDisplayName(0));
                            if (!sourceContainer.isRunning()) {
                                sourceContainer.start();
                            }
                            // create the postgres extension if specified
                            if (sourceType.is(SourceType.POSTGRES)) {
                                PostgresExtensionUtils.createExtension(source, postgresExtension);
                            }
                        },
                        (AfterEachCallback) context -> {
                            connectContainer.deleteAllConnectors();
                            source.waitUntilDeleted();

                            // drop the postgres extension if specified
                            if (sourceType.is(SourceType.POSTGRES)) {
                                PostgresExtensionUtils.dropExtension(source, postgresExtension);
                            }

                            source.close();

                            if (context.getExecutionException().isPresent()) {
                                LOGGER.error("Test {}.{}: {} failed with exception:", testClass.getName(), method.getName(),
                                        getDisplayName(0), context.getExecutionException().get());
                            }
                        },
                        new ParameterResolver() {
                            @Override
                            public boolean supportsParameter(ParameterContext parameterContext,
                                                             ExtensionContext extensionContext)
                                    throws ParameterResolutionException {
                                return parameterContext.getParameter().getType() == Source.class;
                            }

                            @Override
                            public Object resolveParameter(ParameterContext parameterContext,
                                                           ExtensionContext extensionContext)
                                    throws ParameterResolutionException {
                                return source;
                            }
                        });
            }
        };
    }

    @SuppressWarnings("resource")
    private KafkaContainer getKafkaContainer() {
        return DebeziumKafkaContainer.defaultKafkaContainer(network).withNetworkAliases("kafka");
    }

    @SuppressWarnings("resource")
    private DebeziumContainer getKafkaConnectContainer(KafkaContainer kafkaContainer) {
        return DebeziumContainer.nightly()
                .withKafka(kafkaContainer)
                .withNetwork(network)
                .withNetworkAliases("connect")
                .dependsOn(kafkaContainer);
    }

    @SuppressWarnings("resource")
    private Map<SourceType, JdbcDatabaseContainer<?>> getSourceContainers() {
        final Map<SourceType, JdbcDatabaseContainer<?>> containers = new LinkedHashMap<>();
        for (SourceType sourceType : getSourcesToStart()) {
            if (SourceType.MYSQL.equals(sourceType)) {
                final JdbcDatabaseContainer<?> container = new MySQLContainer<>(
                        DockerImageName.parse(MYSQL_IMAGE_NAME).asCompatibleSubstituteFor("mysql"))
                        .withNetwork(network)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withNetworkAliases(sourceType.getValue())
                        .withEnv("TZ", TestHelper.getSourceTimeZone());
                if (TestHelper.isConnectionTimeZoneUsed()) {
                    container.withUrlParam("connectionTimeZone", TestHelper.getSourceTimeZone());
                }
                containers.put(sourceType, container);
            }
            else if (SourceType.POSTGRES.equals(sourceType)) {
                final JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>(
                        DockerImageName.parse(POSTGRES_IMAGE_NAME).asCompatibleSubstituteFor("postgres"))
                        .withNetwork(network)
                        .withUsername(POSTGRES_USERNAME)
                        .withPassword(POSTGRES_PASSWORD)
                        .withNetworkAliases(sourceType.getValue())
                        .withEnv("TZ", TestHelper.getSourceTimeZone())
                        .withEnv("PGTZ", TestHelper.getSourceTimeZone());
                containers.put(sourceType, container);
            }
            else if (SourceType.SQLSERVER.equals(sourceType)) {
                final JdbcDatabaseContainer<?> container = new MSSQLServerContainer<>(
                        DockerImageName.parse(SQLSERVER_IMAGE_NAME))
                        .withNetwork(network)
                        .acceptLicense()
                        .withEnv("MSSQL_AGENT_ENABLED", "true")
                        .withEnv("MSSQL_PID", "Standard")
                        .withPassword(SQLSERVER_PASSWORD)
                        .withInitScript("database-init-scripts/sqlserver-source-init.sql")
                        .withNetworkAliases(sourceType.getValue())
                        .withEnv("TZ", TestHelper.getSourceTimeZone());
                containers.put(sourceType, container);
            }
            else if (SourceType.ORACLE.equals(sourceType)) {
                final JdbcDatabaseContainer<?> container = new OracleContainer(
                        DockerImageName.parse(ORACLE_IMAGE_NAME).asCompatibleSubstituteFor("gvenzl/oracle-xe"))
                        .withNetwork(network)
                        .withUsername(ORACLE_USERNAME)
                        .withPassword(ORACLE_PASSWORD)
                        .withDatabaseName("ORCLPDB1")
                        .withNetworkAliases(sourceType.getValue())
                        .withEnv("TZ", TestHelper.getSourceTimeZone());
                containers.put(sourceType, container);
            }
            // else if (SourceType.DB2.equals(sourceType)) {
            // final JdbcDatabaseContainer<?> container = new Db2Container(DockerImageName.parse(DB2_IMAGE_NAME))
            // .withNetwork(network)
            // .acceptLicense()
            // .withNetworkAliases(sourceType.getValue());
            // containers.put(sourceType, container);
            // }
        }
        return containers;
    }

    /**
     * Returns the source connectors to start. If the {@code source.connectors} environment property
     * is not set, the pipeline defaults to using a specific set of connectors; otherwise the pipeline
     * will only run those specified via the environment variable.
     */
    private List<SourceType> getSourcesToStart() {
        final String sourceConnectors = System.getProperty("source.connectors");
        if (sourceConnectors == null || sourceConnectors.isEmpty()) {
            // By default, the ORACLE source connector is not tested.
            return List.of(SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER);
        }
        else {
            return Arrays.stream(sourceConnectors.split(",")).map(SourceType::parse).collect(Collectors.toList());
        }
    }

}
