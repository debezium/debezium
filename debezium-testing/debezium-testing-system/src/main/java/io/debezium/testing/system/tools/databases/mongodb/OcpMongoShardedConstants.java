/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

public class OcpMongoShardedConstants {
    public static final String MONGO_CONFIG_DEPLOYMENT_NAME = "mongo-config";
    public static final String MONGO_MONGOS_DEPLOYMENT_NAME = "mongo-mongos";
    public static final String MONGO_SHARD_DEPLOYMENT_PREFIX = "mongo-shard";
    public static final int MONGO_MONGOS_PORT = 27017;
    public static final int MONGO_SHARD_PORT = 27018;
    public static final int MONGO_CONFIG_PORT = 27019;
    public static final int SHARD_COUNT = 3;
    public static final int REPLICAS_IN_SHARD = 2;
}
