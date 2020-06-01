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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.Response;

/**
 *
 * @author Jakub Cechacek
 */
public class SqlServerController extends SqlDatabaseController {
    private static class SqlServerInitListener implements ExecListener {
        @Override
        public void onOpen(Response response) {
            LOGGER.info("Initializing Sqlserver database");
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            LOGGER.error("Error initializing Sqlserver database");
            LOGGER.error(response.message());
        }

        @Override
        public void onClose(int code, String reason) {
            LOGGER.info("Sqlserver init executor close: [" + code + "] " + reason);
        }
    }

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

    public void initialize() {
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .file(DB_INIT_SCRIPT_PATH_CONTAINER)
                .upload(initScript);

        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .inContainer("sqlserver")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new SqlServerInitListener())
                .exec("/opt/mssql-tools/bin/sqlcmd", "-U", "sa", "-P", "Debezium1$", "-i", "/opt/inventory.sql")) { // TODO: hard-coded password
        }
    }
}
