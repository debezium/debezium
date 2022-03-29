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

import io.strimzi.api.kafka.model.connect.build.*;
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
import io.strimzi.api.kafka.model.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplateBuilder;

public class OcpKafkaConnectSpecBuilder extends KafkaConnectSpecBuilder {

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
        List<String> databases = ImmutableList.of("mysql", "postgres", "mongodb", "db2", "sqlserver", "oracle");
        String as_url = System.getProperty("as.url");
        String as_debezium_version = System.getProperty("as.debezium.version");
        for (String db : databases) {
            PluginBuilder builder = new PluginBuilder()
                    .withName("debezium-connector-" + db)
                    .withArtifacts(
                            new ZipArtifactBuilder()
                                    .withUrl(String.format("%s/debezium-connector-%s-%s-plugin.zip", as_url, db, as_debezium_version))
                                    .build(),
                            new ZipArtifactBuilder()
                                    .withUrl(System.getProperty("as.apicurio.url"))
                                    .build(),
                            new ZipArtifactBuilder()
                                    .withUrl(String.format("%s/debezium-scripting-%s.zip", as_url, as_debezium_version))
                                    .build());
            if ("oracle".equals(db)) {
                builder.addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/ojdbc8-%s.jar", as_url, System.getProperty("version.oracle.driver")))
                        .build());
            } else if ("db2".equals(db)) {
                builder.addToArtifacts(new JarArtifactBuilder()
                        .withUrl(String.format("%s/jdbc/jcc-%s.jar", as_url, System.getProperty("version.db2.driver")))
                        .build());
            }
            plugins.add(builder.build());

        }

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
