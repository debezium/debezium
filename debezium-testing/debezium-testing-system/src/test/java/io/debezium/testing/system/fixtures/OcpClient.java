/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(provides = { OpenShiftClient.class })
public class OcpClient extends TestFixture {
    private DefaultOpenShiftClient client;

    public OcpClient(@NotNull ExtensionContext.Store store) {
        super(store);
    }

    @Override
    public void setup() {
        Config cfg = new ConfigBuilder()
                .withMasterUrl(ConfigProperties.OCP_URL)
                .withUsername(ConfigProperties.OCP_USERNAME)
                .withPassword(ConfigProperties.OCP_PASSWORD)
                .withTrustCerts(true)
                .build();

        client = new DefaultOpenShiftClient(cfg);
        store(OpenShiftClient.class, client);
    }

    @Override
    public void teardown() {
        if (client != null) {
            client.close();
        }
    }
}
