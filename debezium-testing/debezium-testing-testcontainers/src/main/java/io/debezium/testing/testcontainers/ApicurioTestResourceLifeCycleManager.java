/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class ApicurioTestResourceLifeCycleManager implements QuarkusTestResourceLifecycleManager {

    private static final ApicurioRegistryContainer container = new ApicurioRegistryContainer();
    private static final int APICURIO_PORT = 8080;

    @Override
    public Map<String, String> start() {
        container.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.format.apicurio.registry.url", getApicurioUrl());
        params.put("debezium.format.apicurio.registry.auto-register", "true");
        params.put("debezium.format.apicurio.registry.find-latest", "true");
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

    public static String getApicurioUrl() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(APICURIO_PORT) + "/apis/registry/v2";
    }
}
