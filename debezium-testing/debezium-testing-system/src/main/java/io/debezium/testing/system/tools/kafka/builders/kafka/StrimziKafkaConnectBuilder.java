/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.ARTIFACT_SERVER_APC_URL;
import static io.debezium.testing.system.tools.ConfigProperties.ARTIFACT_SERVER_DB2_DRIVER_VERSION;
import static io.debezium.testing.system.tools.ConfigProperties.ARTIFACT_SERVER_DBZ_VERSION;
import static io.debezium.testing.system.tools.ConfigProperties.ARTIFACT_SERVER_ORACLE_DRIVER_VERSION;
import static io.debezium.testing.system.tools.ConfigProperties.ARTIFACT_SERVER_URL;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ClientTls;
import io.strimzi.api.kafka.model.ClientTlsBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplateBuilder;

/**
 * This class simplifies building of kafkaConnect by providing pre-made configurations for whole kafkaConnect or parts of its definition
 */
public class StrimziKafkaConnectBuilder extends StrimziBuilderWrapper<StrimziKafkaConnectBuilder, KafkaConnectBuilder, KafkaConnect> {
    public static String DEFAULT_KC_NAME = "debezium-kafka-connect-cluster";
    public static String DEFAULT_BOOSTRAP_SERVER = StrimziKafkaBuilder.DEFAULT_KAFKA_NAME + "-kafka-bootstrap:9093";

    protected StrimziKafkaConnectBuilder(KafkaConnectBuilder builder) {
        super(builder);
    }

    @Override
    public KafkaConnect build() {
        return builder.build();
    }

    public boolean hasBuild() {
        return builder.editSpec().hasBuild();
    }

    public Optional<String> imageStream() {
        if (!hasBuild()) {
            return Optional.empty();
        }
        String image = builder.editSpec().editBuild().buildOutput().getImage();
        return Optional.of(image);
    }

    public static StrimziKafkaConnectBuilder base() {
        Map<String, Object> config = defaultConfig();
        KafkaConnectTemplate template = defaultTemplate();
        ClientTls tls = defaultTLS();

        KafkaConnectBuilder builder = new KafkaConnectBuilder()
                .withNewMetadata()
                .withName(DEFAULT_KC_NAME)
                .endMetadata()
                .withNewSpec()
                .withBootstrapServers(DEFAULT_BOOSTRAP_SERVER)
                .withTemplate(template)
                .withConfig(config)
                .withReplicas(1)
                .withTls(tls)
                .endSpec();

        return new StrimziKafkaConnectBuilder(builder);
    }

    public StrimziKafkaConnectBuilder withImage(String image) {
        builder.editSpec().withImage(image).endSpec();
        return self();
    }

    public StrimziKafkaConnectBuilder withBuild() {
        builder
                .editSpec()
                .withNewBuild()
                .withNewImageStreamOutput()
                .withImage("testing-openshift-connect:latest")
                .endImageStreamOutput()
                .endBuild()
                .endSpec();

        return self();
    }

    public StrimziKafkaConnectBuilder withConnectorResources() {
        builder
                .editMetadata()
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata();
        return self();
    }

    public StrimziKafkaConnectBuilder withStandardPlugins() {
        Map<String, PluginBuilder> pluginBuilders = Stream.of("mysql", "postgres", "mongodb", "sqlserver", "db2", "oracle")
                .collect(toMap(identity(), StrimziKafkaConnectBuilder::prepareStandardPluginBuilder));

        pluginBuilders.get("db2")
                .addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/jcc-%s.jar", ARTIFACT_SERVER_URL, ARTIFACT_SERVER_DB2_DRIVER_VERSION))
                        .build())
                .build();
        pluginBuilders.get("oracle")
                .addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/ojdbc8-%s.jar", ARTIFACT_SERVER_URL, ARTIFACT_SERVER_ORACLE_DRIVER_VERSION))
                        .build())
                .build();

        List<Plugin> plugins = pluginBuilders.values().stream()
                .map(PluginBuilder::build)
                .collect(Collectors.toList());

        builder.editSpec().editBuild().withPlugins(plugins).endBuild().endSpec();

        return self();
    }

    public StrimziKafkaConnectBuilder withPullSecret(String pullSecretName) {
        if (builder.editSpec().hasImage()) {
            builder
                    .editSpec()
                    .editTemplate()
                    .editOrNewPod()
                    .addNewImagePullSecret(pullSecretName)
                    .endPod()
                    .endTemplate()
                    .endSpec();
        }

        if (builder.editSpec().hasBuild()) {
            builder
                    .editSpec()
                    .editTemplate()
                    .editOrNewBuildConfig()
                    .withPullSecret(pullSecretName)
                    .endBuildConfig()
                    .endTemplate()
                    .endSpec();
        }

        return self();
    }

    public StrimziKafkaConnectBuilder withLoggingFromConfigMap(ConfigMap configMap) {
        ConfigMapKeySelector configMapKeySelector = new ConfigMapKeySelectorBuilder()
                .withKey("log4j.properties")
                .withName(configMap.getMetadata().getName())
                .build();

        builder
                .editSpec()
                .withNewExternalLogging()
                .withNewValueFrom()
                .withConfigMapKeyRef(configMapKeySelector)
                .endValueFrom()
                .endExternalLogging()
                .endSpec();

        return self();

    }

    public StrimziKafkaConnectBuilder withMetricsFromConfigMap(ConfigMap configMap) {
        ConfigMapKeySelector configMapKeySelector = new ConfigMapKeySelectorBuilder()
                .withKey("metrics")
                .withName(configMap.getMetadata().getName())
                .build();

        builder
                .editSpec()
                .withNewJmxPrometheusExporterMetricsConfig()
                .withNewValueFrom()
                .withConfigMapKeyRef(configMapKeySelector)
                .endValueFrom()
                .endJmxPrometheusExporterMetricsConfig()
                .endSpec();

        return self();
    }

    private static PluginBuilder prepareStandardPluginBuilder(String dbName) {
        return new PluginBuilder()
                .withName("debezium-connector-" + dbName)
                .withArtifacts(
                        new ZipArtifactBuilder()
                                .withUrl(String.format("%s/debezium-connector-%s-%s-plugin.zip", ARTIFACT_SERVER_URL, dbName,
                                        ARTIFACT_SERVER_DBZ_VERSION))
                                .build(),
                        new ZipArtifactBuilder()
                                .withUrl(ARTIFACT_SERVER_APC_URL)
                                .build(),
                        new ZipArtifactBuilder()
                                .withUrl(String.format("%s/debezium-scripting-%s.zip", ARTIFACT_SERVER_URL, ARTIFACT_SERVER_DBZ_VERSION))
                                .build());
    }

    private static KafkaConnectTemplate defaultTemplate() {
        return new KafkaConnectTemplateBuilder().withConnectContainer(new ContainerTemplateBuilder()
                .withEnv(new ContainerEnvVarBuilder()
                        .withName("JMX_PORT")
                        .withValue("5000")
                        .build())
                .build())
                .build();
    }

    private static ClientTls defaultTLS() {
        return new ClientTlsBuilder()
                .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName("debezium-kafka-cluster-cluster-ca-cert")
                                .build())
                .build();
    }

    private static Map<String, Object> defaultConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("config.storage.replication.factor", 1);
        config.put("offset.storage.replication.factor", 1);
        config.put("status.storage.replication.factor", 1);

        return config;
    }
}
