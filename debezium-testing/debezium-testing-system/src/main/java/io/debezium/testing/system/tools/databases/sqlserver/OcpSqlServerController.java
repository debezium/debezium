/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.sqlserver;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_SQLSERVER_SA_PASSWORD;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class OcpSqlServerController extends OcpSqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpSqlServerController.class);
    private static final String DB_INIT_SCRIPT_PATH = "/database-resources/sqlserver/inventory.sql";
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/tmp/inventory.sql";

    private final Path initScript;

    public OcpSqlServerController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, "sqlserver", ocp);
        try {
            initScript = Paths.get(getClass().getResource(DB_INIT_SCRIPT_PATH).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getPublicDatabaseUrl() {
        return "jdbc:" + getDatabaseType() + "://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort()
                + ";encrypt=false";
    }

    public void initialize() throws InterruptedException {
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .file(DB_INIT_SCRIPT_PATH_CONTAINER)
                .upload(initScript);
        ocpUtils.executeCommand(deployment, project, true, "/opt/mssql-tools/bin/sqlcmd", "-U", "sa", "-P", DATABASE_SQLSERVER_SA_PASSWORD, "-i",
                DB_INIT_SCRIPT_PATH_CONTAINER);
    }
}
