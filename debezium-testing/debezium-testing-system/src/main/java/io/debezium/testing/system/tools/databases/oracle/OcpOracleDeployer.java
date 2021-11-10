/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.oracle;

import java.util.List;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public class OcpOracleDeployer extends AbstractOcpDatabaseDeployer<OcpOracleController> {

    public OcpOracleDeployer(String project, Deployment deployment, Secret pullSecret,
                             List<Service> services, OpenShiftClient ocp) {
        super(project, deployment, services, pullSecret, ocp);
    }

    @Override
    protected OcpOracleController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpOracleController(deployment, services, ocp);
    }

    public static class Builder extends DatabaseBuilder<Builder, OcpOracleDeployer> {
        @Override
        public OcpOracleDeployer build() {
            return new OcpOracleDeployer(
                    project,
                    deployment,
                    pullSecret,
                    services,
                    ocpClient);
        }
    }
}
