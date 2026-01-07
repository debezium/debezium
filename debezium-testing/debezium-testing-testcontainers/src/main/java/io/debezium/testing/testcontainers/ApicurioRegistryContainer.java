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
    public static final String APICURIO_REGISTRY_IMAGE = "quay.io/apicurio/apicurio-registry-mem";

    public ApicurioRegistryContainer() {
        super(APICURIO_REGISTRY_IMAGE + ":" + APICURIO_VERSION);

        this.waitStrategy = new LogMessageWaitStrategy()
                .withRegEx(".*apicurio-registry-app.*started in.*");

        addExposedPort(APICURIO_PORT);
    }

    public static String getApicurioVersion() {
        String apicurioVersionTestProperty = System.getProperty(TEST_PROPERTY_PREFIX + "apicurio.version");
        return apicurioVersionTestProperty != null ? apicurioVersionTestProperty
                : ContainerImageVersions.getStableVersion(APICURIO_REGISTRY_IMAGE);
    }
}
