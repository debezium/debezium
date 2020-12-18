package io.debezium.testing.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {

    private static final String APICURIO_VERSION = "1.3.2.Final" ;
    private static GenericContainer<?> apicurioContainer;

    private ApicurioRegistryContainer() {
        apicurioContainer = new GenericContainer<>("apicurio/apicurio-registry-mem:" + APICURIO_VERSION)
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