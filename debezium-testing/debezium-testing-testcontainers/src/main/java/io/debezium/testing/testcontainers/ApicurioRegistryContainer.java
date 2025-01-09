/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import io.debezium.testing.testcontainers.util.ContainerImageVersions;

public class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {

    private static final String APICURIO_VERSION = getApicurioVersion();
    private static final Integer APICURIO_PORT = 8080;
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    public ApicurioRegistryContainer() {
        super("apicurio/apicurio-registry-mem:" + APICURIO_VERSION);

        this.waitStrategy = new LogMessageWaitStrategy()
                .withRegEx(".*apicurio-registry-app.*started in.*");

        addExposedPort(APICURIO_PORT);
    }

    public static String getApicurioVersion() {
        String apicurioVersionTestProperty = System.getProperty(TEST_PROPERTY_PREFIX + "apicurio.version");
        return apicurioVersionTestProperty != null ? apicurioVersionTestProperty
                : ContainerImageVersions.getStableVersion("quay.io/apicurio/apicurio-registry-mem");
    }
}
