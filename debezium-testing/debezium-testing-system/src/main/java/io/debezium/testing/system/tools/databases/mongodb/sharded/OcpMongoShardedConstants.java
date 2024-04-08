/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

public class OcpMongoShardedConstants {
    public static final String MONGO_CONFIG_DEPLOYMENT_NAME = "mongo-config";
    public static final String MONGO_CONFIG_REPLICASET_NAME = "mongo-config";
    public static final String MONGO_SHARD_DEPLOYMENT_PREFIX = "mongo-shard";
    public static final String MONGO_CONFIG_ROLE = "mongo-config";
    public static final String MONGO_MONGOS_ROLE = "mongo-mongos";
    public static final String MONGO_SHARD_ROLE = "mongo-shard";

    public static final int MONGO_MONGOS_PORT = 27017;
    public static final int MONGO_SHARD_PORT = 27018;
    public static final int MONGO_CONFIG_PORT = 27019;
    public static final int SHARD_COUNT = 2;
    public static final int REPLICAS_IN_SHARD = 3;
    public static final int CONFIG_SERVER_REPLICAS = 3;

    public static final String ADMIN_DB = "admin";

    public final static String INIT_RS_TEMPLATE = "init-rs.js";
    public final static String CREATE_CERT_USER_TEMPLATE = "create-dbz-user-x509.js";
    public final static String CREATE_DBZ_USER_TEMPLATE = "create-dbz-user.js";
    public final static String INSERT_MONGOS_DATA_SCRIPT_LOC = "/database-resources/mongodb/sharded/insert-mongos-data.js";
    public final static String KEYFILE_PATH_IN_CONTAINER = "/etc/mongodb.keyfile";
}
