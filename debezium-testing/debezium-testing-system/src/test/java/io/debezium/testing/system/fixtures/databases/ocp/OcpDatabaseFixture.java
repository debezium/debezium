/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.fixtures.databases.DatabaseFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.DatabaseController;
import io.fabric8.openshift.client.OpenShiftClient;

public abstract class OcpDatabaseFixture<T extends DatabaseController<?>> extends DatabaseFixture<T> {

    protected final OpenShiftClient ocp;

    public OcpDatabaseFixture(Class<T> controllerType, ExtensionContext.Store store) {
        super(controllerType, store);
        this.ocp = retrieve(OpenShiftClient.class);
    }

    @Override
    public void teardown() throws Exception {
        if (dbController != null && !ConfigProperties.PREPARE_NAMESPACES_AND_STRIMZI) {
            dbController.reload();
        }
    }
}
