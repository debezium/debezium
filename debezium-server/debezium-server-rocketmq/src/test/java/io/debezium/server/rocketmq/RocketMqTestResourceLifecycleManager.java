/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a RocketMQ cluster test resource.
 */
public class RocketMqTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static RocketMqContainer container = new RocketMqContainer();
    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static synchronized void init() {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        init();
        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.rocketmq.producer.name.srv.addr", getNameSrvAddr());
        params.put("debezium.sink.rocketmq.producer.group", "producer-group");
        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static String getNameSrvAddr() {
        return container.getNamesrvAddr();
    }
}
