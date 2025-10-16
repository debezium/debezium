/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaClusterUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterUtils.class);

    /**
     * Create a Kafka topic using AdminClient.
     *
     * @param topicName the name of the topic to create
     * @param partitions the number of partitions
     * @param replicationFactor the replication factor
     * @throws Exception if topic creation fails
     */
    public static void createTopic(String topicName, int partitions, short replicationFactor, String bootstrapServers) throws Exception {
        Properties adminProperties = new Properties();
        adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProperties.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "test-admin-client-" + UUID.randomUUID().toString());

        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            try {
                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
                result.all().get(30, TimeUnit.SECONDS);
                LOGGER.info("Topic '{}' created successfully", topicName);
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    LOGGER.info("Topic '{}' already exists", topicName);
                }
                else {
                    throw e;
                }
            }
        }
    }
}
