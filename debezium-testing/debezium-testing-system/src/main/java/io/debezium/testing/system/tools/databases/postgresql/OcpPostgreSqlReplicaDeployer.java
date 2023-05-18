/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.postgresql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpPostgreSqlReplicaDeployer extends AbstractOcpDatabaseDeployer<OcpPostgreSqlReplicaController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpPostgreSqlDeployer.class);

    public OcpPostgreSqlReplicaDeployer(
                                        String project,
                                        Deployment deployment,
                                        List<Service> services,
                                        OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    @Override
    public OcpPostgreSqlReplicaController getController(
                                                        Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpPostgreSqlReplicaController(deployment, services, "postgresql", ocp);
    }

    public static class Deployer extends AbstractOcpDatabaseDeployer.DatabaseBuilder<OcpPostgreSqlReplicaDeployer.Deployer, OcpPostgreSqlReplicaDeployer> {
        @Override
        public OcpPostgreSqlReplicaDeployer build() {
            return new OcpPostgreSqlReplicaDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }
}
