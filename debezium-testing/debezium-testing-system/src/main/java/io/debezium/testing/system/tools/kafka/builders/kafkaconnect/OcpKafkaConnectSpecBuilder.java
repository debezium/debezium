/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders.kafkaconnect;

import static io.debezium.testing.system.tools.kafka.builders.kafkaconnect.OcpKafkaConnectBuilder.DEFAULT_BOOSTRAP_SERVER;
import static io.debezium.testing.system.tools.kafka.builders.kafkaconnect.OcpKafkaConnectBuilder.DEFAULT_IMAGE;
import static io.debezium.testing.system.tools.kafka.builders.kafkaconnect.OcpKafkaConnectBuilder.DEFAULT_KAFKA_CONNECT_VERSION;

import java.util.LinkedList;
import java.util.List;

import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ClientTlsBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.ExternalConfigurationReferenceBuilder;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.connect.build.BuildBuilder;
import io.strimzi.api.kafka.model.connect.build.ImageStreamOutputBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplateBuilder;

public class OcpKafkaConnectSpecBuilder extends KafkaConnectSpecBuilder {

    private static final String AS_APICURIO_URL = System.getProperty("as.apicurio.url");
    private static final String AS_DEBEZIUM_VERSION = System.getProperty("as.debezium.version");
    private static final String AS_URL = System.getProperty("as.url");
    private static final String DEBEZIUM_CONNECTOR_PLUGIN_NAME_PREFIX = "debezium-connector-";

    public OcpKafkaConnectSpecBuilder withNonKcSetup() {
        return this.withDefaultVersion()
                .withDefaultImage()
                .withDefaultBoostrapServer()
                .withDefaultLogging()
                .withDefaultTls()
                .withDefaultReplicas()
                .withDefaultMetrics()
                .withDefaultConfig()
                .withDefaultTemplate();
    }

    public OcpKafkaConnectSpecBuilder withKcSetup() {
        List<Plugin> plugins = new LinkedList<>();
        List<String> dbsWithDefaultPlugin = ImmutableList.of("mysql", "postgres", "mongodb", "sqlserver");

        for (String db : dbsWithDefaultPlugin) {
            plugins.add(prepareStandardPluginBuilder(db).build());
        }
        plugins.add(prepareStandardPluginBuilder("db2")
                .addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/jcc-%s.jar", AS_URL, System.getProperty("version.db2.driver")))
                        .build())
                .build());
        plugins.add(prepareStandardPluginBuilder("oracle")
                .addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/ojdbc8-%s.jar", AS_URL, System.getProperty("version.oracle.driver")))
                        .build())
                .build());

        return (OcpKafkaConnectSpecBuilder) this.withDefaultVersion()
                .withDefaultTemplate()
                .withDefaultConfig()
                .withDefaultMetrics()
                .withDefaultReplicas()
                .withDefaultTls()
                .withDefaultLogging()
                .withDefaultBoostrapServer()
                .withBuild(new BuildBuilder()
                        .withOutput(new ImageStreamOutputBuilder()
                                .withImage("testing-openshift-connect:latest")
                                .build())
                        .withPlugins(plugins)
                        .build());

    }

    private PluginBuilder prepareStandardPluginBuilder(String dbName) {
        return new PluginBuilder()
                .withName(DEBEZIUM_CONNECTOR_PLUGIN_NAME_PREFIX + dbName)
                .withArtifacts(
                        new ZipArtifactBuilder()
                                .withUrl(String.format("%s/debezium-connector-%s-%s-plugin.zip", AS_URL, dbName, AS_DEBEZIUM_VERSION))
                                .build(),
                        new ZipArtifactBuilder()
                                .withUrl(AS_APICURIO_URL)
                                .build(),
                        new ZipArtifactBuilder()
                                .withUrl(String.format("%s/debezium-scripting-%s.zip", AS_URL, AS_DEBEZIUM_VERSION))
                                .build());
    }

    public OcpKafkaConnectSpecBuilder withDefaultVersion() {
        return (OcpKafkaConnectSpecBuilder) this.withVersion(DEFAULT_KAFKA_CONNECT_VERSION);
    }

    public OcpKafkaConnectSpecBuilder withDefaultTemplate() {
        return (OcpKafkaConnectSpecBuilder) this.withTemplate(
                new KafkaConnectTemplateBuilder().withConnectContainer(new ContainerTemplateBuilder()
                        .withEnv(new ContainerEnvVarBuilder()
                                .withName("JMX_PORT")
                                .withValue("5000")
                                .build())
                        .build())
                        .build());
    }

    public OcpKafkaConnectSpecBuilder withDefaultImage() {
        return (OcpKafkaConnectSpecBuilder) this.withImage(DEFAULT_IMAGE);
    }

    public OcpKafkaConnectSpecBuilder withDefaultBoostrapServer() {
        return (OcpKafkaConnectSpecBuilder) this.withBootstrapServers(DEFAULT_BOOSTRAP_SERVER);
    }

    public OcpKafkaConnectSpecBuilder withDefaultLogging() {
        return (OcpKafkaConnectSpecBuilder) this.withLogging(new ExternalLoggingBuilder().withNewValueFromLike(new ExternalConfigurationReferenceBuilder()
                .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                        .withKey("log4j.properties")
                        .withName("connect-cfg")
                        .build())
                .build())
                .endValueFrom()
                .build());
    }

    public OcpKafkaConnectSpecBuilder withDefaultTls() {
        return (OcpKafkaConnectSpecBuilder) this.withTls(new ClientTlsBuilder()
                .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName("debezium-kafka-cluster-cluster-ca-cert")
                                .build())
                .build());
    }

    public OcpKafkaConnectSpecBuilder withDefaultReplicas() {
        return (OcpKafkaConnectSpecBuilder) this.withReplicas(1);
    }

    public OcpKafkaConnectSpecBuilder withDefaultMetrics() {
        return (OcpKafkaConnectSpecBuilder) this.withMetricsConfig(
                new JmxPrometheusExporterMetricsBuilder().withNewValueFromLike(new ExternalConfigurationReferenceBuilder()
                        .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                .withKey("metrics")
                                .withName("connect-cfg")
                                .build())
                        .build())
                        .endValueFrom()
                        .build());
    }

    public OcpKafkaConnectSpecBuilder withDefaultConfig() {
        return (OcpKafkaConnectSpecBuilder) this.withConfig(ImmutableMap.of(
                "config.storage.replication.factor", 1,
                "offset.storage.replication.factor", 1,
                "status.storage.replication.factor", 1));
    }
}
