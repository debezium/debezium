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

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.tools.databases.DatabaseController;
import io.fabric8.openshift.client.OpenShiftClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class OcpConnectorTest<D extends DatabaseController<?>>
        extends ConnectorTest<D>
        implements OcpKafka, OcpClient {

    // OpenShift control
    protected OpenShiftClient ocp;
    Logger LOGGER = LoggerFactory.getLogger(OcpConnectorTest.class);

    @Override
    @BeforeAll
    public void setupFixtures() throws Exception {
        setupOcpClient();
        super.setupFixtures();
    }

    @Override
    @AfterAll
    public void teardownFixtures() throws Exception {
        super.teardownFixtures();
        teardownOcpClient();
    }

    @Override
    public OpenShiftClient getOcpClient() {
        return this.ocp;
    }

    @Override
    public void setOcpClient(OpenShiftClient client) {
        this.ocp = client;
    }
}
