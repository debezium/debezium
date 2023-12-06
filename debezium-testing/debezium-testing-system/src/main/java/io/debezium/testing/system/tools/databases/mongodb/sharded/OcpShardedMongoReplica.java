/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

import lombok.Getter;

@Getter
public class OcpShardedMongoReplica extends OcpMongoShardedNode {
    private final int replicaNum;

    public OcpShardedMongoReplica(Deployment deployment, Service service, String serviceUrl, OpenShiftClient ocp, String project, int replicaNum) {
        super(deployment, service, serviceUrl, ocp, project);
        this.replicaNum = replicaNum;
    }
}
