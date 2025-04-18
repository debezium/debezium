/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.assertions.JdbcAssertions;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mariadb.MariaDbController;
import io.debezium.testing.system.tools.databases.mariadb.OcpMariaDbDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { MariaDbController.class, JdbcAssertions.class })
public class OcpMariaDb extends OcpDatabaseFixture<MariaDbController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mariadb/deployment.yml";
    public static final String DB_SERVICE_PATH = "/database-resources/mariadb/service.yml";
    private static final String DB_VOLUME_CLAIM_PATH = "/database-resources/mariadb/volume-claim.yml";

    public OcpMariaDb(ExtensionContext.Store store) {
        super(MariaDbController.class, store);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        store(JdbcAssertions.class, new JdbcAssertions(dbController));
    }

    @Override
    protected MariaDbController databaseController() throws Exception {
        Class.forName("org.mariadb.jdbc.Driver");
        OcpMariaDbDeployer deployer = new OcpMariaDbDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MARIADB)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withVolumeClaim(DB_VOLUME_CLAIM_PATH)
                .withPullSecrets(ConfigProperties.OCP_PULL_SECRET_PATH.get())
                .withServices(DB_SERVICE_PATH)
                .build();
        return deployer.deploy();
    }
}
