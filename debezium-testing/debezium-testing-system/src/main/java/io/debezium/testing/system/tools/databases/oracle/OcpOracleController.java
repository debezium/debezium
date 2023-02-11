/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.oracle;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PDBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_USERNAME;
import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.databases.DatabaseInitListener;
import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpOracleController extends OcpSqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpOracleController.class);
    private static final String DB_INIT_SCRIPT_PATH = "/database-resources/oracle/inventory.sql";
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/home/oracle/inventory.sql";

    private final Path initScript;

    public OcpOracleController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, "oracle", ocp);
        try {
            initScript = Paths.get(getClass().getResource(DB_INIT_SCRIPT_PATH).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void initialize() throws InterruptedException {
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        LOGGER.info("Uploading inventory.sql to " + DB_INIT_SCRIPT_PATH_CONTAINER);
        ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .file(DB_INIT_SCRIPT_PATH_CONTAINER)
                .upload(initScript);

        CountDownLatch latch = new CountDownLatch(1);
        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .inContainer("oracle")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new DatabaseInitListener("oracle", latch))
                .exec("sqlplus", "-S",
                        DATABASE_ORACLE_USERNAME + "/" + DATABASE_ORACLE_PASSWORD + "@//localhost:1521/ORCLPDB1", "@" + DB_INIT_SCRIPT_PATH_CONTAINER)) {
            LOGGER.info("Waiting until database is initialized");
            latch.await(WaitConditions.scaled(1), TimeUnit.MINUTES);
        }
    }

    @Override
    public String getPublicDatabaseUrl() {
        return "jdbc:oracle:thin:@" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort() + "/" + DATABASE_ORACLE_PDBNAME;
    }
}
