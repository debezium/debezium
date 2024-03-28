/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.util.Objects;

public class ConnectorDescriptor {

    private final String id;
    private final String displayName;
    private final String className;
    private final String version;

    private ConnectorDescriptor(String id, String displayName, String className, String version) {
        this.id = id;
        this.displayName = displayName;
        this.className = className;
        this.version = version;
    }

    public ConnectorDescriptor(String className, String version) {
        this(getIdForConnectorClass(className), getDisplayNameForConnectorClass(className), className, version);
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
            case "io.debezium.connector.mariadb.MariaDbConnector":
                return "mariadb";
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
            case "io.debezium.connector.mariadb.MariaDbConnector":
                return "Debezium MariaDB Connector";
            default:
                throw new RuntimeException("Unsupported connector type with className: \"" + className + "\"");
        }
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        return this.getClassName().equals(((ConnectorDescriptor) that).getClassName())
                && this.getVersion().equals(((ConnectorDescriptor) that).getVersion());
    }

    public int hashCode() {
        return Objects.hash(this.className, this.version);
    }
}
