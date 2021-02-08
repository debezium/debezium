/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.assertions.AvroKafkaAssertions;
import io.debezium.testing.openshift.assertions.KafkaAssertions;
import io.debezium.testing.openshift.assertions.PlainKafkaAssertions;
import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.kafka.KafkaConnectController;
import io.debezium.testing.openshift.tools.kafka.KafkaController;
import io.debezium.testing.openshift.tools.kafka.KafkaDeployer;
import io.debezium.testing.openshift.tools.kafka.OperatorController;
import io.debezium.testing.openshift.tools.registry.RegistryController;
import io.debezium.testing.openshift.tools.registry.RegistryDeployer;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * @author Jakub Cechacek
 */
public abstract class ConnectorTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorTestBase.class);

    protected static OpenShiftClient ocp;
    protected static TestUtils testUtils;
    protected static OkHttpClient httpClient = new OkHttpClient();
    protected static KafkaAssertions assertions;

    // Kafka resources
    public static final String KAFKA = "/kafka-resources/010-kafka.yaml";
    public static final String KAFKA_CONNECT_S2I_LOGGING = "/kafka-resources/020-kafka-connect-logging.yaml";
    public static final String KAFKA_CONNECT_S2I = "/kafka-resources/021-kafka-connect.yaml";

    // Service registry resources
    public static final String REGISTRY_DEPLOYMENT_PATH = "/registry-resources/030-registry-streams.yaml";
    public static final String REGISTRY_STORAGE_TOPIC_PATH = "/registry-resources/010-storage-topic.yaml";
    public static final String REGISTRY_ID_TOPIC_PATH = "/registry-resources/020-global-id-topic.yaml";

    protected static Properties KAFKA_CONSUMER_PROPS = new Properties();

    // Kafka Control
    protected static KafkaDeployer kafkaDeployer;
    protected static KafkaController kafkaController;
    protected static KafkaConnectController kafkaConnectController;
    protected static OperatorController operatorController;

    // Registry Control
    protected static RegistryDeployer registryDeployer;
    protected static RegistryController registryController;

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

        deployKafkaCluster();
        initKafkaConsumerProps();

        if (ConfigProperties.DEPLOY_SERVICE_REGISTRY) {
            // deploy registry
            registryDeployer = new RegistryDeployer(ConfigProperties.OCP_PROJECT_REGISTRY, ocp, httpClient, kafkaController);
            registryController = registryDeployer.deployRegistry(REGISTRY_DEPLOYMENT_PATH, REGISTRY_STORAGE_TOPIC_PATH, REGISTRY_ID_TOPIC_PATH);
            assertions = new AvroKafkaAssertions(KAFKA_CONSUMER_PROPS);
        }
        else {
            assertions = new PlainKafkaAssertions(KAFKA_CONSUMER_PROPS);
        }
    }

    private static void deployKafkaCluster() throws InterruptedException {
        kafkaDeployer = new KafkaDeployer(ConfigProperties.OCP_PROJECT_DBZ, ocp);

        operatorController = kafkaDeployer.getOperator();
        operatorController.setLogLevel("DEBUG");
        operatorController.setAlwaysPullPolicy();
        operatorController.setOperandAlwaysPullPolicy();
        operatorController.setSingleReplica();

        if (ConfigProperties.OCP_PULL_SECRET_PATHS.isPresent()) {
            String paths = ConfigProperties.OCP_PULL_SECRET_PATHS.get();
            LOGGER.info("Processing pull secrets: " + paths);

            List<String> secrets = Arrays.stream(paths.split(","))
                    .map(kafkaDeployer::deployPullSecret)
                    .map(s -> s.getMetadata().getName())
                    .collect(Collectors.toList());

            secrets.forEach(operatorController::setImagePullSecret);
            operatorController.setOperandImagePullSecrets(String.join(",", secrets));
        }

        operatorController.updateOperator();

        kafkaController = kafkaDeployer.deployKafkaCluster(KAFKA);
        kafkaConnectController = kafkaDeployer.deployKafkaConnectCluster(KAFKA_CONNECT_S2I, KAFKA_CONNECT_S2I_LOGGING, ConfigProperties.STRIMZI_OPERATOR_CONNECTORS);

        kafkaConnectController.allowServiceAccess();
        kafkaConnectController.exposeApi();
        kafkaConnectController.exposeMetrics();
    }

    private static void initKafkaConsumerProps() {
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaController.getKafkaBootstrapAddress());
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.GROUP_ID_CONFIG, "DEBEZIUM_IT_01");
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KAFKA_CONSUMER_PROPS.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    @AfterAll
    public static void teardown() {
        ocp.close();
    }

}
