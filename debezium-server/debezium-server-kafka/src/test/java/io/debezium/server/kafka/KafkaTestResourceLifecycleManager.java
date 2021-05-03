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

    @SuppressWarnings("deprecation")
    private static KafkaContainer kafkaContainer = new KafkaContainer();

    @Override
    public Map<String, String> start() {
        kafkaContainer.start();
        return new HashMap<>();
    }

    @Override
    public void stop() {
        kafkaContainer.stop();
    }

    public static String getBootstrapServers() {
        // if container is already started, start() will return early
        kafkaContainer.start();
        return kafkaContainer.getBootstrapServers();
    }
}
