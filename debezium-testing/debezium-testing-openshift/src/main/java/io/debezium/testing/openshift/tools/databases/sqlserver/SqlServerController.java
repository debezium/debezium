/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.sqlserver;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.WaitConditions;
import io.debezium.testing.openshift.tools.databases.DatabaseInitListener;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class SqlServerController extends SqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerController.class);
    private static final String DB_INIT_SCRIPT_PATH = "/database-resources/sqlserver/inventory.sql";
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/opt/inventory.sql";

    private final Path initScript;

    public SqlServerController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
        try {
            initScript = Paths.get(getClass().getResource(DB_INIT_SCRIPT_PATH).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String constructDatabaseUrl(String hostname, int port) {
        return "jdbc:" + dbType + "://" + hostname + ":" + port;
    }

    public void initialize() throws InterruptedException {
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .file(DB_INIT_SCRIPT_PATH_CONTAINER)
                .upload(initScript);

        CountDownLatch latch = new CountDownLatch(1);
        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .inContainer("sqlserver")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new DatabaseInitListener("sqlserver", latch))
                .exec("/opt/mssql-tools/bin/sqlcmd", "-U", "sa", "-P", "Debezium1$", "-i", "/opt/inventory.sql")) { // TODO: hard-coded password
            LOGGER.info("Waiting until database is initialized");
            latch.await(WaitConditions.scaled(1), TimeUnit.MINUTES);
        }
    }
}
