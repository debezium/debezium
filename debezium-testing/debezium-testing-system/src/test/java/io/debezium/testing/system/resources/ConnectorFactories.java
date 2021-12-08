/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.resources;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
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
        int dbPort = controller.getDatabasePort();

        return cb
                .put("database.server.name", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_MYSQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_MYSQL_DBZ_PASSWORD)
                .put("database.history.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }

    public ConnectorConfigBuilder postgresql(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("database.server.name", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_POSTGRESQL_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_POSTGRESQL_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME)
                .put("database.dbname", ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME)
                .put("slot.name", "debezium")
                .put("plugin.name", "pgoutput");
    }

    public ConnectorConfigBuilder sqlserver(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("database.server.name", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_SQLSERVER_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_SQLSERVER_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_SQLSERVER_DBZ_DBNAME)
                .put("database.history.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }

    public ConnectorConfigBuilder mongo(MongoDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("mongodb.name", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .put("task.max", 1)
                .put("mongodb.hosts", "rs0/" + dbHost + ":" + dbPort)
                .put("mongodb.user", ConfigProperties.DATABASE_MONGO_DBZ_USERNAME)
                .put("mongodb.password", ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD);
    }

    public ConnectorConfigBuilder db2(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("database.server.name", cb.getDbServerName())
                .put("connector.class", "io.debezium.connector.db2.Db2Connector")
                .put("task.max", 1)
                .put("database.hostname", dbHost)
                .put("database.port", dbPort)
                .put("database.user", ConfigProperties.DATABASE_DB2_DBZ_USERNAME)
                .put("database.password", ConfigProperties.DATABASE_DB2_DBZ_PASSWORD)
                .put("database.dbname", ConfigProperties.DATABASE_DB2_DBZ_DBNAME)
                .put("database.cdcschema", ConfigProperties.DATABASE_DB2_CDC_SCHEMA)
                .put("database.history.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("database.history.kafka.topic", "schema-changes.inventory");
    }

    public ConnectorConfigBuilder oracle(SqlDatabaseController controller, String connectorName) {
        ConnectorConfigBuilder cb = new ConnectorConfigBuilder(connectorName);
        String dbHost = controller.getDatabaseHostname();
        int dbPort = controller.getDatabasePort();

        return cb
                .put("database.server.name", cb.getDbServerName())
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
                .put("database.history.kafka.bootstrap.servers", kafka.getBootstrapAddress())
                .put("database.history.kafka.topic", "schema-changes.oracle")
                .put("log.mining.strategy", "online_catalog");
    }
}
