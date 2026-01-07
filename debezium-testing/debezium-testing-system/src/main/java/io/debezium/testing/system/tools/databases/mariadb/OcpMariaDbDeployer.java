/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mariadb;

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
public final class OcpMariaDbDeployer extends AbstractOcpDatabaseDeployer<MariaDbController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMariaDbDeployer.class);
    private final PersistentVolumeClaim volumeClaim;

    private OcpMariaDbDeployer(
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
    public MariaDbController deploy() {
        PersistentVolumeClaim ocpPvc = ocp.persistentVolumeClaims().inNamespace(ConfigProperties.OCP_PROJECT_MARIADB)
                .withName(volumeClaim.getMetadata().getName()).get();
        if (ocpPvc == null) {
            LOGGER.info("Deploying persistent volume claim");
            ocp.persistentVolumeClaims().inNamespace(ConfigProperties.OCP_PROJECT_MARIADB).createOrReplace(volumeClaim);
        }
        else {
            LOGGER.info("Persistent volume claim already exists. Skipping deployment");
        }
        return super.deploy();
    }

    @Override
    public OcpMariaDbController getController(
                                              Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMariaDbController(deployment, services, "mariadb", ocp);
    }

    public static class Deployer extends DatabaseBuilder<Deployer, OcpMariaDbDeployer> {
        private PersistentVolumeClaim volumeClaim;

        public Deployer withVolumeClaim(String dbVolumeClaimPath) {
            this.volumeClaim = YAML.fromResource(dbVolumeClaimPath, PersistentVolumeClaim.class);
            return self();
        }

        @Override
        public OcpMariaDbDeployer build() {
            return new OcpMariaDbDeployer(
                    project,
                    deployment,
                    services,
                    pullSecret,
                    volumeClaim,
                    ocpClient);
        }
    }
}
