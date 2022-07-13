/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(provides = { OpenShiftClient.class })
public class OcpClient extends TestFixture {
    private DefaultOpenShiftClient client;

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpClient.class);

    public OcpClient(@NotNull ExtensionContext.Store store) {
        super(store);
    }

    @Override
    public void setup() {
        ConfigBuilder configBuilder = new ConfigBuilder();
        if (!isRunningFromOcp()) {
            LOGGER.info("Running outside OCP, using OCP credentials passed from parameters");
            configBuilder.withMasterUrl(ConfigProperties.OCP_URL.get())
                    .withUsername(ConfigProperties.OCP_USERNAME.get())
                    .withPassword(ConfigProperties.OCP_PASSWORD.get());
        }
        configBuilder.withRequestRetryBackoffLimit(ConfigProperties.OCP_REQUEST_RETRY_BACKOFF_LIMIT)
                .withTrustCerts(true);

        client = new DefaultOpenShiftClient(configBuilder.build());
        store(OpenShiftClient.class, client);
    }

    @Override
    public void teardown() {
        if (client != null) {
            client.close();
        }
    }
}
