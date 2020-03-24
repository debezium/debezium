/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.resources;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jakub Cechacek
 */
public class ConfigProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigProperties.class);

    public static final String OCP_URL = stringProperty("test.ocp.url");
    public static final String OCP_USERNAME = stringProperty("test.ocp.username");
    public static final String OCP_PASSWORD = stringProperty("test.ocp.password");
    public static final String OCP_PROJECT_DBZ = stringProperty("test.ocp.project.debezium");
    public static final String OCP_PROJECT_MYSQL = System.getProperty("test.ocp.project.mysql", "debezium-mysql");
    public static final String OCP_PROJECT_POSTGRESQL = System.getProperty("test.ocp.project.postgresql", "debezium-postgresql");
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

    private static String stringProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            LOGGER.error("Undefined property " + key);
            throw new IllegalStateException("Undefined property \"" + key + "\"");
        }
        return value;
    }
}
