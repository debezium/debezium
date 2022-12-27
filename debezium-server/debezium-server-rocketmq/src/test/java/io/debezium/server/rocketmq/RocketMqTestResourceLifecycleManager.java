/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.util.HashMap;
import java.util.Map;

import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a RocketMQ cluster test resource.
 */
public class RocketMqTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static RocketMqContainer rocketMQContainer = new RocketMqContainer(DockerImageName.parse("apache/rocketmq:4.9.4"));

    @Override
    public Map<String, String> start() {
        rocketMQContainer.start();
        return new HashMap<>();
    }

    @Override
    public void stop() {
        rocketMQContainer.stop();
    }

    public static String getNamesrvAddr() {
        rocketMQContainer.start();
        return rocketMQContainer.getNamesrvAddr();
    }

    public static String getGroup() {
        return rocketMQContainer.getGroup();
    }
}
