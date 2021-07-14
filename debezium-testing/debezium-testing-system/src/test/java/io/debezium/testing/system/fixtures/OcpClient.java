/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import io.debezium.testing.system.tools.ConfigProperties;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;

public interface OcpClient extends TestSetupFixture {

    default void setupOcpClient() {
        Config cfg = new ConfigBuilder()
                .withMasterUrl(ConfigProperties.OCP_URL)
                .withUsername(ConfigProperties.OCP_USERNAME)
                .withPassword(ConfigProperties.OCP_PASSWORD)
                .withTrustCerts(true)
                .build();
        setOcpClient(new DefaultOpenShiftClient(cfg));
    }

    default void teardownOcpClient() {
        OpenShiftClient ocp = getOcpClient();
        if (ocp != null) {
            ocp.close();
        }
    }

    void setOcpClient(OpenShiftClient client);

    OpenShiftClient getOcpClient();
}
