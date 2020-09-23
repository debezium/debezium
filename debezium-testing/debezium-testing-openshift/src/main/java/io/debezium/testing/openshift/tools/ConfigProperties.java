/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jakub Cechacek
 */
public class ConfigProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigProperties.class);

    public static final long WAIT_SCALE_FACTOR = longProperty("test.wait.scale", 1);

    public static final String OCP_URL = stringProperty("test.ocp.url");
    public static final String OCP_USERNAME = stringProperty("test.ocp.username");
    public static final String OCP_PASSWORD = stringProperty("test.ocp.password");
    public static final String OCP_PROJECT_DBZ = stringProperty("test.ocp.project.debezium");
    public static final String OCP_PROJECT_MYSQL = System.getProperty("test.ocp.project.mysql", "debezium-mysql");
    public static final String OCP_PROJECT_POSTGRESQL = System.getProperty("test.ocp.project.postgresql", "debezium-postgresql");
    public static final String OCP_PROJECT_SQLSERVER = System.getProperty("test.ocp.project.sqlserver", "debezium-sqlserver");
    public static final String OCP_PROJECT_MONGO = System.getProperty("test.ocp.project.mongo", "debezium-mongo");
    public static final String OCP_PROJECT_DB2 = System.getProperty("test.ocp.project.db2", "debezium-db2");
    public static final Optional<String> OCP_PULL_SECRET_PATHS = stringOptionalProperty("test.ocp.pull.secret.paths");

    public static final boolean STRIMZI_OPERATOR_CONNECTORS = booleanProperty("test.strimzi.operator.connectors");

    public static final int DATABASE_MYSQL_PORT = Integer.parseInt(System.getProperty("test.database.mysql.port", "3306"));
    public static final String DATABASE_MYSQL_USERNAME = System.getProperty("test.database.mysql.username", "mysqluser");
    public static final String DATABASE_MYSQL_PASSWORD = System.getProperty("test.database.mysql.password", "mysqlpw");
    public static final String DATABASE_MYSQL_DBZ_USERNAME = System.getProperty("test.database.mysql.dbz.username", "debezium");
    public static final String DATABASE_MYSQL_DBZ_PASSWORD = System.getProperty("test.database.mysql.dbz.password", "dbz");
    public static final Optional<String> DATABASE_MYSQL_HOST = stringOptionalProperty("test.database.mysql.host");

    public static final int DATABASE_POSTGRESQL_PORT = Integer.parseInt(System.getProperty("test.database.postgresql.port", "5432"));
    public static final String DATABASE_POSTGRESQL_USERNAME = System.getProperty("test.database.postgresql.username", "debezium");
    public static final String DATABASE_POSTGRESQL_PASSWORD = System.getProperty("test.database.postgresql.password", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_USERNAME = System.getProperty("test.database.postgresql.dbz.username", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_PASSWORD = System.getProperty("test.database.postgresql.dbz.password", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_DBNAME = System.getProperty("test.database.postgresql.dbname", "debezium");
    public static final Optional<String> DATABASE_POSTGRESQL_HOST = stringOptionalProperty("test.database.postgresql.host");

    public static final int DATABASE_SQLSERVER_PORT = Integer.parseInt(System.getProperty("test.database.sqlserver.port", "1433"));
    public static final String DATABASE_SQLSERVER_USERNAME = System.getProperty("test.database.sqlserver.username", "sa");
    public static final String DATABASE_SQLSERVER_SA_PASSWORD = System.getProperty("test.database.sqlserver.password", "Debezium1$");
    public static final String DATABASE_SQLSERVER_DBZ_USERNAME = System.getProperty("test.database.sqlserver.dbz.username", DATABASE_SQLSERVER_USERNAME);
    public static final String DATABASE_SQLSERVER_DBZ_PASSWORD = System.getProperty("test.database.sqlserver.dbz.password", DATABASE_SQLSERVER_SA_PASSWORD);
    public static final String DATABASE_SQLSERVER_DBZ_DBNAME = System.getProperty("test.database.sqlserver.dbname", "testDB");
    public static final Optional<String> DATABASE_SQLSERVER_HOST = stringOptionalProperty("test.database.sqlserver.host");

    public static final int DATABASE_MONGO_PORT = Integer.parseInt(System.getProperty("test.database.mongo.port", "27017"));
    public static final String DATABASE_MONGO_USERNAME = System.getProperty("test.database.mongo.username", "admin");
    public static final String DATABASE_MONGO_SA_PASSWORD = System.getProperty("test.database.mongo.password", "admin");
    public static final String DATABASE_MONGO_DBZ_USERNAME = System.getProperty("test.database.mongo.dbz.username", "debezium");
    public static final String DATABASE_MONGO_DBZ_PASSWORD = System.getProperty("test.database.mongo.dbz.password", "dbz");
    public static final String DATABASE_MONGO_DBZ_DBNAME = System.getProperty("test.database.mongo.dbname", "inventory");
    public static final String DATABASE_MONGO_DBZ_LOGIN_DBNAME = System.getProperty("test.database.mongo.dbz.login.dbname", "admin");
    public static final Optional<String> DATABASE_MONGO_HOST = stringOptionalProperty("test.database.mongo.host");

    public static final int DATABASE_DB2_PORT = Integer.parseInt(System.getProperty("test.database.db2.port", "50000"));
    public static final String DATABASE_DB2_USERNAME = System.getProperty("test.database.db2.username", "db2inst1");
    public static final String DATABASE_DB2_PASSWORD = System.getProperty("test.database.db2.password", "=Password!");
    public static final String DATABASE_DB2_DBZ_USERNAME = System.getProperty("test.database.db2.dbz.username", DATABASE_DB2_USERNAME);
    public static final String DATABASE_DB2_DBZ_PASSWORD = System.getProperty("test.database.db2.dbz.password", DATABASE_DB2_PASSWORD);
    public static final String DATABASE_DB2_DBZ_DBNAME = System.getProperty("test.database.db2.dbname", "TESTDB");
    public static final String DATABASE_DB2_CDC_SCHEMA = System.getProperty("test.database.db2.cdc.schema", "ASNCDC");
    public static final Optional<String> DATABASE_DB2_HOST = stringOptionalProperty("test.database.sqlserver.host");

    private static boolean booleanProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("false") || value.equalsIgnoreCase("0")) {
            return false;
        }
        return true;
    }

    private static Optional<String> stringOptionalProperty(String key) {
        String value = System.getProperty(key);
        return Optional.ofNullable((value == null || value.isEmpty()) ? null : value);
    }

    private static long longProperty(String key, long defaultValue) {
        String value = System.getProperty(key);
        return (value == null || value.isEmpty()) ? defaultValue : Long.parseLong(value);
    }

    private static String stringProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            LOGGER.error("Undefined property " + key);
            throw new IllegalStateException("Undefined property \"" + key + "\"");
        }
        return value;
    }
}
