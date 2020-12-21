package io.debezium.testing.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {

    private static final String DEBEZIUM_VERSION = DebeziumContainer.getDebeziumStableVersion();
    private static GenericContainer<?> apicurioContainer;

    private ApicurioRegistryContainer() {
        apicurioContainer = new GenericContainer<>("apicurio/apicurio-registry-mem:" + DEBEZIUM_VERSION)
                .withExposedPorts(8080)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*apicurio-registry-app.*started in.*"));
    }

    public static ApicurioRegistryContainer getApicurioRegistryContainer() {
        return new ApicurioRegistryContainer();
    }

    public GenericContainer<?> getApicurioContainer() {
        return apicurioContainer;
    }
}
