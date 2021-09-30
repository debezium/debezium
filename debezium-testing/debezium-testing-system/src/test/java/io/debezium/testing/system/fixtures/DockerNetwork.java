/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import org.testcontainers.containers.Network;

public interface DockerNetwork {

    default void setupNetwork() {
        setNetwork(Network.newNetwork());
    }

    default void teardownNetwork() {
        getNetwork().close();
    }

    void setNetwork(Network network);

    Network getNetwork();
}
