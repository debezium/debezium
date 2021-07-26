/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.kafka.DockerKafka;
import io.debezium.testing.system.tools.databases.DatabaseController;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DockerConnectorTest<D extends DatabaseController<?>>
        extends ConnectorTest<D>
        implements DockerKafka, DockerNetwork {

    // Docker control
    protected Network network;
    Logger LOGGER = LoggerFactory.getLogger(DockerConnectorTest.class);

    @Override
    @BeforeAll
    public void setupFixtures() throws Exception {
        setupNetwork();
        super.setupFixtures();
    }

    @Override
    @AfterAll
    public void teardownFixtures() throws Exception {
        super.teardownFixtures();
        teardownNetwork();
    }

    @Override
    public Network getNetwork() {
        return network;
    }

    @Override
    public void setNetwork(Network network) {
        this.network = network;
    }
}
