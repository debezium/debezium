/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import java.util.List;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMySqlReplicaDeployer extends AbstractOcpDatabaseDeployer<MySqlReplicaController> {
    public OcpMySqlReplicaDeployer(String project, Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    @Override
    protected MySqlReplicaController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMySqlController(deployment, services, "mysql", ocp);
    }

    public static class Deployer extends DatabaseBuilder<OcpMySqlReplicaDeployer.Deployer, OcpMySqlReplicaDeployer> {
        @Override
        public OcpMySqlReplicaDeployer build() {
            return new OcpMySqlReplicaDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }
}
