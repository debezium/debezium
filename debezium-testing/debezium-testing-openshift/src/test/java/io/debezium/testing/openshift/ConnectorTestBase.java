/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.resources.ConfigProperties;
import io.debezium.testing.openshift.tools.kafka.KafkaConnectController;
import io.debezium.testing.openshift.tools.kafka.KafkaController;
import io.debezium.testing.openshift.tools.kafka.KafkaDeployer;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public abstract class ConnectorTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorTestBase.class);

    public static final String KAFKA = "/kafka-resources/010-kafka.yaml";
    public static final String KAFKA_CONNECT_S2I_LOGGING = "/kafka-resources/020-kafka-connect-logging.yaml";
    public static final String KAFKA_CONNECT_S2I = "/kafka-resources/021-kafka-connect.yaml";

    protected static Properties KAFKA_CONSUMER_PROPS = new Properties();
    protected static OpenShiftClient ocp;
    protected static TestUtils testUtils;
    protected static KafkaDeployer kafkaDeployer;
    protected static KafkaController kafkaController;
    protected static KafkaConnectController kafkaConnectController;

    @BeforeAll
    public static void setup() throws InterruptedException {
        Config cfg = new ConfigBuilder()
                .withMasterUrl(ConfigProperties.OCP_URL)
                .withUsername(ConfigProperties.OCP_USERNAME)
                .withPassword(ConfigProperties.OCP_PASSWORD)
                .withTrustCerts(true)
                .build();
        ocp = new DefaultOpenShiftClient(cfg);
        testUtils = new TestUtils();

        kafkaDeployer = new KafkaDeployer(ConfigProperties.OCP_PROJECT_DBZ, ocp);
        ConfigProperties.OCP_SECRET_RHIO_PATH.ifPresent(kafkaDeployer::deployPullSecret);

        kafkaController = kafkaDeployer.deployKafkaCluster(KAFKA);
        kafkaConnectController = kafkaDeployer.deployKafkaConnectCluster(KAFKA_CONNECT_S2I, KAFKA_CONNECT_S2I_LOGGING, ConfigProperties.STRIMZI_OPERATOR_CONNECTORS);
        kafkaConnectController.allowServiceAccess();
        kafkaConnectController.exposeApi();
        kafkaConnectController.exposeMetrics();

        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaController.getKafkaBootstrapAddress());
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.GROUP_ID_CONFIG, "DEBEZIUM_IT_01");
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    protected void assertTopicsExist(String... names) {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(KAFKA_CONSUMER_PROPS)) {
            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                Set<String> topics = consumer.listTopics().keySet();
                assertThat(topics).contains(names);
            });
        }
    }

    protected void assertRecordsCount(String topic, int count) {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(KAFKA_CONSUMER_PROPS)) {
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            consumer.seekToBeginning(consumer.assignment());
            assertThat(records.count()).isEqualTo(count);
        }
    }
}
