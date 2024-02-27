/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded.freemarker;

import java.util.List;
import java.util.stream.Collectors;

import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoReplicaSetMember;

public class InitReplicaSetModel {
    private final List<OcpMongoReplicaSetMember> members;
    private final String rsId;
    private final boolean configServer;

    public String getMembers() {
        return members.stream().map(en -> "{ _id: " + en.getReplicaNum() + ", host: \"" + en.getServiceUrl() + "\" }").collect(Collectors.joining(","));
    }

    public String getRsId() {
        return rsId;
    }

    public boolean isConfigServer() {
        return configServer;
    }

    public InitReplicaSetModel(List<OcpMongoReplicaSetMember> members, String rsId, boolean configServer) {
        this.members = members;
        this.rsId = rsId;
        this.configServer = configServer;
    }
}
