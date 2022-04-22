/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class PubSubTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:380.0.0-emulators"));
    private static String endpoint;

    @Override
    public Map<String, String> start() {
        emulator.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        endpoint = emulator.getEmulatorEndpoint();
        params.put("debezium.sink.pubsub.address", endpoint);
        return params;
    }

    @Override
    public void stop() {
        try {
            if (emulator != null) {
                emulator.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static String getEmulatorEndpoint() {
        return endpoint;
    }
}
