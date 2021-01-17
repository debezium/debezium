/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.pulsar;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class PulsarContainer extends GenericContainer<PulsarContainer> {

    private static final String PULSAR_VERSION = "2.5.2";
    public static final int PULSAR_PORT = 6650;

    private static final DockerImageName PULSAR_IMAGE_NAME = DockerImageName.parse("apachepulsar/pulsar").withTag(PULSAR_VERSION);

    public PulsarContainer() {
        super(PULSAR_IMAGE_NAME);

        this.withExposedPorts(PULSAR_PORT);
    }

    public String getPulsarServiceUrl() {
        return "pulsar://localhost:" + getMappedPort(PULSAR_PORT);
    }

}
