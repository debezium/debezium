/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.resources;

import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MONGO_HOST;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MYSQL_HOST;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_POSTGRESQL_HOST;

import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

/**
 *
 * @author Jakub Cechacek
 */
public class ConnectorFactories {

    public ConnectorConfigBuilder mysql() {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder();
        String dbHost = DATABASE_MYSQL_HOST.orElse("mysql." + ConfigProperties.OCP_PROJECT_MYSQL + ".svc.cluster.local");
        return cb
                .put("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", ConfigProperties.DATABASE_MYSQL_PORT)
                .put("database.user", ConfigProperties.DATABASE_MYSQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_MYSQL_DBZ_PASSWORD)
                .put("database.server.name", "mysqldb") // this should be overwritten with unique name
                .put("database.history.kafka.bootstrap.servers", "debezium-kafka-cluster-kafka-bootstrap." + ConfigProperties.OCP_PROJECT_DBZ + ".svc.cluster.local:9092")
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }

    public ConnectorConfigBuilder postgresql() {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder();
        String dbHost = DATABASE_POSTGRESQL_HOST.orElse("postgresql." + ConfigProperties.OCP_PROJECT_POSTGRESQL + ".svc.cluster.local");
        return cb
                .put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", ConfigProperties.DATABASE_POSTGRESQL_PORT)
                .put("database.user", ConfigProperties.DATABASE_POSTGRESQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_POSTGRESQL_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME)
                .put("database.server.name", "postgresqldb") // this should be overwritten with unique name
                .put("slot.name", "debezium")
                .put("plugin.name", "pgoutput");
    }

    public ConnectorConfigBuilder sqlserver() {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder();
        String dbHost = DATABASE_POSTGRESQL_HOST.orElse("sqlserver." + ConfigProperties.OCP_PROJECT_SQLSERVER + ".svc.cluster.local");
        return cb
                .put("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", ConfigProperties.DATABASE_SQLSERVER_PORT)
                .put("database.user", ConfigProperties.DATABASE_SQLSERVER_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_SQLSERVER_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_SQLSERVER_DBZ_DBNAME)
                .put("database.server.name", "sqlserverdb") // this should be overwritten with unique name
                .put("database.history.kafka.bootstrap.servers", "debezium-kafka-cluster-kafka-bootstrap." + ConfigProperties.OCP_PROJECT_DBZ + ".svc.cluster.local:9092")
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }

    public ConnectorConfigBuilder mongo() {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder();
        String dbHost = DATABASE_MONGO_HOST.orElse("mongo." + ConfigProperties.OCP_PROJECT_MONGO + ".svc.cluster.local");
        return cb
                .put("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .put("task.max", 1)
                .put("mongodb.hosts", "rs0/" + dbHost + ":" + ConfigProperties.DATABASE_MONGO_PORT)
                .put("mongodb.user", ConfigProperties.DATABASE_MONGO_DBZ_USERNAME)
                .put("mongodb.password", ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD)
                .put("mongodb.name", "mongodb"); // this should be overwritten with unique name
    }

    public ConnectorConfigBuilder db2() {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder();
        String dbHost = DATABASE_POSTGRESQL_HOST.orElse("db2." + ConfigProperties.OCP_PROJECT_DB2 + ".svc.cluster.local");
        return cb
                .put("connector.class", "io.debezium.connector.db2.Db2Connector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", ConfigProperties.DATABASE_DB2_PORT)
                .put("database.user", ConfigProperties.DATABASE_DB2_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_DB2_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_DB2_DBZ_DBNAME)
                .put("database.cdcschema", "ASNCDC")
                .put("database.server.name", "db2db") // this should be overwritten with unique name
                .put("database.history.kafka.bootstrap.servers", "debezium-kafka-cluster-kafka-bootstrap." + ConfigProperties.OCP_PROJECT_DBZ + ".svc.cluster.local:9092")
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }
}
