/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public final class OcpMySqlDeployer extends AbstractOcpDatabaseDeployer<MySqlController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMySqlDeployer.class);
    private final PersistentVolumeClaim volumeClaim;

    private OcpMySqlDeployer(
                                   String project,
                                   Deployment deployment,
                                   List<Service> services,
                                   Secret pullSecret,
                                   PersistentVolumeClaim volumeClaim,
                                   OpenShiftClient ocp) {
        super(project, deployment, services, pullSecret, ocp);
        this.volumeClaim = volumeClaim;
    }

    @Override
    public MySqlController deploy() {
        LOGGER.info("Deploying persistent volume claim");
        ocp.persistentVolumeClaims().inNamespace(ConfigProperties.OCP_PROJECT_MYSQL).createOrReplace(volumeClaim);
        return super.deploy();
    }

    @Override
    public OcpMySqlController getController(
                                            Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMySqlController(deployment, services, "mysql", ocp);
    }

    public static class Deployer extends DatabaseBuilder<Deployer, OcpMySqlDeployer> {
        private PersistentVolumeClaim volumeClaim;

        public Deployer withVolumeClaim(String dbVolumeClaimPath) {
            this.volumeClaim = YAML.fromResource(dbVolumeClaimPath, PersistentVolumeClaim.class);
            return self();
        }

        @Override
        public OcpMySqlDeployer build() {
            return new OcpMySqlDeployer(
                    project,
                    deployment,
                    services,
                    pullSecret,
                    volumeClaim,
                    ocpClient);
        }
    }
}
