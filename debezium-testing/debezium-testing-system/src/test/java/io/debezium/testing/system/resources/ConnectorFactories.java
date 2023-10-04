/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.resources;

import java.util.Random;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.DockerMongoController;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaController;

/**
 *
 * @author Jakub Cechacek
 */
public class ConnectorFactories {

    private final KafkaController kafka;

    public ConnectorFactories(KafkaController kafka) {
        this.kafka = kafka;
    }

    public ConnectorConfigBuilder mysql(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        Random random = new Random();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("topic.prefix", cb.getDbServerName())
                .put("database.server.id", 5400 + random.nextInt(1000))
                .put("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_MYSQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_MYSQL_DBZ_PASSWORD)
                .put("schema.history.internal.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("schema.history.internal.kafka.topic", "schema-changes.inventory")
                .addOperationRouterForTable("u", "customers");

    }

    public ConnectorConfigBuilder postgresql(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("topic.prefix", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_POSTGRESQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_POSTGRESQL_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME)
                .put("slot.name", "debezium")
                .put("plugin.name", "pgoutput")
                .addOperationRouterForTable("u", "customers");
    }

    public ConnectorConfigBuilder sqlserver(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("topic.prefix", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_SQLSERVER_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_SQLSERVER_DBZ_PASSWORD)
                .put("database.names", ConfigProperties.DATABASE_SQLSERVER_DBZ_DBNAMES)
                .put("database.encrypt", false)
                .put("schema.history.internal.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("schema.history.internal.kafka.topic", "schema-changes.inventory")
                .addOperationRouterForTable("u", "customers");
    }

    public ConnectorConfigBuilder mongo(MongoDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        cb
                .put("topic.prefix", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .put("task.max", 1)
                .put("mongodb.user", ConfigProperties.DATABASE_MONGO_DBZ_USERNAME)
                .put("mongodb.password", ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD)
                .addOperationRouterForTable("u", "customers");

        // Ugly, needs refactoring so that all mongo controllers use connection string
        if (controller instanceof DockerMongoController) {
            // We should be always using this config
            // for OCP public and internal URLs might be different though
            cb.put("mongodb.connection.string", controller.getPublicDatabaseUrl());
        }
        else {
            String dbHost = controller.getDatabaseHostname();
            int dbPort = controller.getDatabasePort();
            cb.put("mongodb.hosts", "rs0/" + dbHost + ":" + dbPort);
        }

        return cb;
    }

    public ConnectorConfigBuilder db2(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("topic.prefix", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.db2.Db2Connector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_DB2_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_DB2_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_DB2_DBZ_DBNAME)
                .put("database.cdcschema", ConfigProperties.DATABASE_DB2_CDC_SCHEMA)
                .put("schema.history.internal.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("schema.history.internal.kafka.topic", "schema-changes.inventory")
                .addOperationRouterForTable("u", "CUSTOMERS");
    }

    public ConnectorConfigBuilder oracle(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("topic.prefix", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.oracle.OracleConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_ORACLE_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_ORACLE_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_ORACLE_DBNAME)
                .put("database.pdb.name", ConfigProperties.DATABASE_ORACLE_PDBNAME)
                .put("schema.include.list", "DEBEZIUM")
                .put("table.include.list", "DEBEZIUM.CUSTOMERS")
                .put("database.pdb.name", ConfigProperties.DATABASE_ORACLE_PDBNAME)
                .put("schema.history.internal.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("schema.history.internal.kafka.topic", "schema-changes.oracle")
                .put("log.mining.strategy", "online_catalog")
                .addOperationRouterForTable("u", "CUSTOMERS");
    }
}
