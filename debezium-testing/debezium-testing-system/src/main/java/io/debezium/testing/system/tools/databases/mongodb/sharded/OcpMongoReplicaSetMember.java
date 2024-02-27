/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Mongo deployment, that is part of a replica set
 */
public class OcpMongoReplicaSetMember extends OcpMongoDeploymentManager {
    private final int replicaNum;

    public OcpMongoReplicaSetMember(Deployment deployment, Service service, String serviceUrl, OpenShiftClient ocp, String project, int replicaNum) {
        super(deployment, service, serviceUrl, ocp, project);
        this.replicaNum = replicaNum;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public static OcpMongoReplicaSetMemberBuilder builder() {
        return new OcpMongoReplicaSetMemberBuilder();
    }

    public static final class OcpMongoReplicaSetMemberBuilder {
        private Deployment deployment;
        private Service service;
        private String serviceUrl;
        private OpenShiftClient ocp;
        private String project;
        private int replicaNum;

        private OcpMongoReplicaSetMemberBuilder() {
        }

        public static OcpMongoReplicaSetMemberBuilder anOcpMongoReplicaSetMember() {
            return new OcpMongoReplicaSetMemberBuilder();
        }

        public OcpMongoReplicaSetMemberBuilder withDeployment(Deployment deployment) {
            this.deployment = deployment;
            return this;
        }

        public OcpMongoReplicaSetMemberBuilder withService(Service service) {
            this.service = service;
            return this;
        }

        public OcpMongoReplicaSetMemberBuilder withServiceUrl(String serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
        }

        public OcpMongoReplicaSetMemberBuilder withOcp(OpenShiftClient ocp) {
            this.ocp = ocp;
            return this;
        }

        public OcpMongoReplicaSetMemberBuilder withProject(String project) {
            this.project = project;
            return this;
        }

        public OcpMongoReplicaSetMemberBuilder withReplicaNum(int replicaNum) {
            this.replicaNum = replicaNum;
            return this;
        }

        public OcpMongoReplicaSetMember build() {
            return new OcpMongoReplicaSetMember(deployment, service, serviceUrl, ocp, project, replicaNum);
        }
    }
}
