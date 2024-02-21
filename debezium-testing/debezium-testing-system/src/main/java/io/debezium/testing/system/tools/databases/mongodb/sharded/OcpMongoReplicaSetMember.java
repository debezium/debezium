/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

import lombok.Builder;
import lombok.Getter;

/**
 * Mongo deployment, that is part of a replica set
 */
@Getter
public class OcpMongoReplicaSetMember extends OcpMongoDeploymentManager {
    private final int replicaNum;

    @Builder(setterPrefix = "with")
    public OcpMongoReplicaSetMember(Deployment deployment, Service service, String serviceUrl, OpenShiftClient ocp, String project, int replicaNum) {
        super(deployment, service, serviceUrl, ocp, project);
        this.replicaNum = replicaNum;
    }
}
