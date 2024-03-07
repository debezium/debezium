/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CERT_FILENAME;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CERT_SECRET;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddress;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicList;

/**
 * This class provides control over Kafka instance deployed in OpenShift
 *
 * @author Jakub Cechacek
 */
public class OcpKafkaController implements KafkaController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaController.class);

    private final OpenShiftClient ocp;
    private final String project;
    private final String name;
    private final StrimziOperatorController operatorController;

    private Kafka kafka;

    public OcpKafkaController(Kafka kafka, StrimziOperatorController operatorController, OpenShiftClient ocp) {
        this.kafka = kafka;
        this.name = kafka.getMetadata().getName();
        this.ocp = ocp;
        this.project = kafka.getMetadata().getNamespace();
        this.operatorController = operatorController;
    }

    @Override
    public String getPublicBootstrapAddress() {
        List<ListenerStatus> listeners = kafka.getStatus().getListeners();
        ListenerStatus listener = listeners.stream()
                .filter(l -> l.getName().equalsIgnoreCase("external"))
                .findAny().orElseThrow(() -> new IllegalStateException("No external listener found for Kafka cluster " + kafka.getMetadata().getName()));
        ListenerAddress address = listener.getAddresses().get(0);
        return address.getHost() + ":" + address.getPort();
    }

    @Override
    public String getBootstrapAddress() {
        return name + "-kafka-bootstrap." + project + ".svc.cluster.local:9092";
    }

    @Override
    public String getTlsBootstrapAddress() {
        return name + "-kafka-bootstrap." + project + ".svc.cluster.local:9093";
    }

    /**
     * Returns bootstrap to be used by KC.
     * The address is local.
     *
     * @return bootstrap
     */
    public String getLocalBootstrapAddress() {
        return name + "-kafka-bootstrap:9093";
    }

    /**
     * Deploy kafka topic from given CR
     *
     * @param yamlPath path to yaml descript
     * @return created topic
     * @throws InterruptedException
     */
    public KafkaTopic deployTopic(String yamlPath) throws InterruptedException {
        LOGGER.info("Deploying Kafka topic from " + yamlPath);
        KafkaTopic topic = topicOperation().createOrReplace(YAML.fromResource(yamlPath, KafkaTopic.class));
        return waitForKafkaTopic(topic.getMetadata().getName());
    }

    @Override
    public boolean undeploy() {
        try {
            Crds.kafkaOperation(ocp).resource(kafka).delete();
            Crds.kafkaOperation(ocp)
                    .resource(kafka)
                    .waitUntilCondition(WaitConditions::resourceDeleted, scaled(1), MINUTES);
        }
        catch (Exception exception) {
            LOGGER.error("Kafka cluster was not deleted");
            return false;
        }
        return true;
    }

    @Override
    public void waitForCluster() throws InterruptedException {
        LOGGER.info("Waiting for Kafka cluster '" + name + "'");
        kafka = kafkaOperation()
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, scaled(7), MINUTES);
    }

    /**
     * Waits until topic is properly deployed.
     *
     * @param name name of the topic
     * @throws InterruptedException     on wait error
     * @throws IllegalArgumentException when deployment doesn't use custom resources
     */
    private KafkaTopic waitForKafkaTopic(String name) throws InterruptedException {
        return topicOperation()
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, scaled(5), MINUTES);
    }

    private NonNamespaceOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation() {
        return Crds.topicOperation(ocp).inNamespace(project);
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }

    @Override
    public Properties getDefaultConsumerProperties() {
        Properties kafkaConsumerProps = new Properties();
        try {
            kafkaConsumerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getKafkaCaCertificate().getAbsolutePath());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        kafkaConsumerProps.put(BOOTSTRAP_SERVERS_CONFIG, getPublicBootstrapAddress());
        kafkaConsumerProps.put(GROUP_ID_CONFIG, "DEBEZIUM_IT_01");
        kafkaConsumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        kafkaConsumerProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        return kafkaConsumerProps;
    }

    @Override
    public Properties getDefaultProducerProperties() {
        Properties kafkaProducerProps = new Properties();
        try {
            kafkaProducerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getKafkaCaCertificate().getAbsolutePath());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        kafkaProducerProps.put(BOOTSTRAP_SERVERS_CONFIG, getPublicBootstrapAddress());
        kafkaProducerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put(ACKS_CONFIG, "all");
        kafkaProducerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        kafkaProducerProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        return kafkaProducerProps;
    }


    private File getKafkaCaCertificate() throws IOException {
        // get kafka cluster ca secret
        Secret secret = ocp.secrets().inNamespace(project).withName(KAFKA_CERT_SECRET).get();
        if (secret == null) {
            throw new IllegalStateException("Kafka cluster certificate secret not found");
        }

        // download and decode certificate
        String cert = secret.getData().get(KAFKA_CERT_FILENAME);
        byte[] decodedBytes = Base64.getDecoder().decode(cert);
        cert = new String(decodedBytes);

        // save to local file
        File crtFile = Files.createTempFile("kafka-cert-", null).toFile();
        Files.writeString(crtFile.toPath(), cert);
        return crtFile;
    }
}
