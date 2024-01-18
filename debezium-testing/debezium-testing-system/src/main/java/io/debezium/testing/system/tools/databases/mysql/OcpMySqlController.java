/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import java.util.List;

import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMySqlController extends OcpSqlDatabaseController implements MySqlController, MySqlReplicaController {
    public OcpMySqlController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
    }
}
