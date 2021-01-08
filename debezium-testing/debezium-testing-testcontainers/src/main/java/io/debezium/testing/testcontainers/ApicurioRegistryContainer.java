/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {

    private static final String DEBEZIUM_VERSION = DebeziumContainer.getDebeziumStableVersion();
    private static final Integer APICURIO_PORT = 8080;

    public ApicurioRegistryContainer() {
        super("apicurio/apicurio-registry-mem:" + DEBEZIUM_VERSION);

        this.waitStrategy = new LogMessageWaitStrategy()
                .withRegEx(".*apicurio-registry-app.*started in.*");

        addExposedPort(APICURIO_PORT);
    }
}
