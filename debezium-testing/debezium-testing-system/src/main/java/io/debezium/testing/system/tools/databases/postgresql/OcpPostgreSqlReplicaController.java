/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.postgresql;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_USERNAME;

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

public class OcpPostgreSqlReplicaController extends OcpSqlDatabaseController {
    private static final String INIT_SCRIPT_PATH = "/database-resources/postgresql/replica/schema_init.sql";
    private static final String INIT_SCRIPT_PATH_CONTAINER = "/init.sql";
    private final Path initScript;

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpPostgreSqlReplicaController.class);

    public OcpPostgreSqlReplicaController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, "postgresql", ocp);
        try {
            initScript = Paths.get(getClass().getResource(INIT_SCRIPT_PATH).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize() throws InterruptedException {
        super.initialize();
        // logical replication on postgresql requires to first create identical schema on replica, so schema creation
        // sql script is copied and executed on replica before starting replication
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .file(INIT_SCRIPT_PATH_CONTAINER)
                .upload(initScript);
        executeInitCommand(deployment, "psql", "-U", DATABASE_POSTGRESQL_USERNAME, "-d", DATABASE_POSTGRESQL_DBZ_DBNAME, "-f", INIT_SCRIPT_PATH_CONTAINER);
    }
}
