/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

public class ConnectorDescriptor {

    private final String id;
    private final String displayName;
    private final String className;
    private final String version;

    public ConnectorDescriptor(String id, String displayName, String className, String version) {
        this.id = id;
        this.displayName = displayName;
        this.className = className;
        this.version = version;
    }

    public ConnectorDescriptor(String className, String version) {
        this.id = getIdForConnectorClass(className);
        this.displayName = getDisplayNameForConnectorClass(className);
        this.className = className;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getClassName() {
        return className;
    }

    public String getVersion() {
        return version;
    }

    public static String getIdForConnectorClass(String className) {
        switch (className) {
            case "io.debezium.connector.mongodb.MongoDbConnector":
                return "mongodb";
            case "io.debezium.connector.mysql.MySqlConnector":
                return "mysql";
            case "io.debezium.connector.oracle.OracleConnector":
                return "oracle";
            case "io.debezium.connector.postgresql.PostgresConnector":
                return "postgres";
            case "io.debezium.connector.sqlserver.SqlServerConnector":
                return "sqlserver";
            default:
                throw new RuntimeException("Unsupported connector type with className: \"" + className + "\"");
        }
    }

    public static String getDisplayNameForConnectorClass(String className) {
        switch (className) {
            case "io.debezium.connector.mongodb.MongoDbConnector":
                return "Debezium MongoDB Connector";
            case "io.debezium.connector.mysql.MySqlConnector":
                return "Debezium MySQL Connector";
            case "io.debezium.connector.oracle.OracleConnector":
                return "Debezium Oracle Connector";
            case "io.debezium.connector.postgresql.PostgresConnector":
                return "Debezium PostgreSQL Connector";
            case "io.debezium.connector.sqlserver.SqlServerConnector":
                return "Debezium SQLServer Connector";
            default:
                throw new RuntimeException("Unsupported connector type with className: \"" + className + "\"");
        }
    }
}
