/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders;

import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaBuilder.DEFAULT_KAFKA_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerController;
import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ClientTls;
import io.strimzi.api.kafka.model.ClientTlsBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplateBuilder;

/**
 * This class simplifies building of kafkaConnect by providing pre-made configurations for whole kafkaConnect or parts of its definition
 */
public class FabricKafkaConnectBuilder extends
        FabricBuilderWrapper<FabricKafkaConnectBuilder, KafkaConnectBuilder, KafkaConnect> {
    public static String DEFAULT_KC_NAME = "debezium-kafka-connect-cluster";
    public static String KAFKA_CERT_SECRET = DEFAULT_KAFKA_NAME + "-cluster-ca-cert";
    public static String KAFKA_CERT_FILENAME = "ca.crt";
    public static String DEFAULT_BOOSTRAP_SERVER = DEFAULT_KAFKA_NAME + "-kafka-bootstrap:9093";

    protected FabricKafkaConnectBuilder(KafkaConnectBuilder builder) {
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

    public static FabricKafkaConnectBuilder base(String bootstrap) {
        Map<String, Object> config = defaultConfig();
        KafkaConnectTemplate template = defaultTemplate();
        ClientTls tls = defaultTLS();

        KafkaConnectBuilder builder = new KafkaConnectBuilder()
                .withNewMetadata()
                .withName(DEFAULT_KC_NAME)
                .endMetadata()
                .withNewSpec()
                .withBootstrapServers(bootstrap)
                .withTemplate(template)
                .withConfig(config)
                .withReplicas(1)
                .withTls(tls)
                .endSpec();

        return new FabricKafkaConnectBuilder(builder);
    }

    public FabricKafkaConnectBuilder withBuild(OcpArtifactServerController artifactServer) {
        List<Plugin> plugins = new ArrayList<>(List.of(
                artifactServer.createDebeziumPlugin("mysql"),
                artifactServer.createDebeziumPlugin("postgres"),
                artifactServer.createDebeziumPlugin("mongodb"),
                artifactServer.createDebeziumPlugin("sqlserver"),
                artifactServer.createDebeziumPlugin("db2", List.of("jdbc/jcc"))));

        if (ConfigProperties.DATABASE_ORACLE) {
            plugins.add(
                    artifactServer.createDebeziumPlugin("oracle", List.of("jdbc/ojdbc8")));
        }

        return withBuild(plugins);
    }

    public FabricKafkaConnectBuilder withBuild(List<Plugin> plugins) {
        builder
                .editSpec()
                .withNewBuild()
                .withNewImageStreamOutput()
                .withImage("testing-openshift-connect:latest")
                .endImageStreamOutput()
                .withPlugins(plugins)
                .endBuild()
                .endSpec();

        return self();
    }

    public FabricKafkaConnectBuilder withConnectorResources(Boolean enabled) {
        return enabled ? withConnectorResources() : self();
    }

    public FabricKafkaConnectBuilder withConnectorResources() {
        builder
                .editMetadata()
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata();
        return self();
    }

    public FabricKafkaConnectBuilder withPullSecret(Optional<Secret> maybePullSecret) {
        maybePullSecret
                .map(s -> s.getMetadata().getName())
                .ifPresent(this::withPullSecret);
        return self();
    }

    public FabricKafkaConnectBuilder withPullSecret(String pullSecretName) {
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

    public FabricKafkaConnectBuilder withLoggingFromConfigMap(ConfigMap configMap) {
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

    public FabricKafkaConnectBuilder withMetricsFromConfigMap(ConfigMap configMap) {
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
                                .withCertificate(KAFKA_CERT_FILENAME)
                                .withSecretName(KAFKA_CERT_SECRET)
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
