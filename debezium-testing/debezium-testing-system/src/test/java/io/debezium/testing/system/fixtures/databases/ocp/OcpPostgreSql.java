/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.postgresql.OcpPostgreSqlDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { SqlDatabaseController.class })
public class OcpPostgreSql extends OcpDatabaseFixture<SqlDatabaseController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/postgresql/deployment.yaml";
    public static final String DB_SERVICE_PATH_LB = "/database-resources/postgresql/service-lb.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/postgresql/service.yaml";

    public OcpPostgreSql(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("org.postgresql.Driver");
        String[] services = isRunningFromOcp() ? new String[]{DB_SERVICE_PATH} : new String[]{DB_SERVICE_PATH, DB_SERVICE_PATH_LB};
        OcpPostgreSqlDeployer deployer = new OcpPostgreSqlDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_POSTGRESQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(services)
                .build();
        return deployer.deploy();
    }
}
