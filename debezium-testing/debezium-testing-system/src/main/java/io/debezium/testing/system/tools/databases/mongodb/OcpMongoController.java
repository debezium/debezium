/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;
import static io.debezium.testing.system.tools.WaitConditions.scaled;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseController;
import io.debezium.testing.system.tools.databases.DatabaseInitListener;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class OcpMongoController
        extends AbstractOcpDatabaseController<MongoDatabaseClient>
        implements MongoDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoController.class);
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/usr/local/bin/init-inventory.sh";

    public OcpMongoController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, ocp);
    }

    @Override
    public String getPublicDatabaseUrl() {
        return "mongodb://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort();
    }

    public void initialize() throws InterruptedException {
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        String svcName = deployment.getMetadata().getName();
        CountDownLatch latch = new CountDownLatch(1);

        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .inContainer("mongo")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new DatabaseInitListener("mongo", latch))
                .exec("bash", "-c", DB_INIT_SCRIPT_PATH_CONTAINER + " -h " + svcName + "." + project + ".svc.cluster.local")) {
            LOGGER.info("Waiting until database is initialized");
            latch.await(scaled(1), TimeUnit.MINUTES);
        }
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, "admin");
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getPublicDatabaseUrl(), username, password, authSource);
    }
}
