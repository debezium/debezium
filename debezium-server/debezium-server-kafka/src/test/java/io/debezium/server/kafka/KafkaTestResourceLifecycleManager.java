/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.HashMap;
import java.util.Map;

import org.testcontainers.containers.KafkaContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a Kafka cluster test resource.
 *
 * @author Alfusainey Jallow
 */
public class KafkaTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static KafkaContainer kafkaContainer = new KafkaContainer();

    @Override
    public Map<String, String> start() {
        kafkaContainer.start();

        Map<String, String> props = new HashMap<>();
        return props;
    }

    @Override
    public void stop() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    public static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
