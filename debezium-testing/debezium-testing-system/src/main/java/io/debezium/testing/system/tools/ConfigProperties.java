/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jakub Cechacek
 */
public final class ConfigProperties {

    private ConfigProperties() {
        // intentionally private
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigProperties.class);

    public static final long WAIT_SCALE_FACTOR = longProperty("test.wait.scale", 1);

    // DockerConfiguration configuration
    public static final String DOCKER_IMAGE_KAFKA_RHEL = System.getProperty("test.docker.image.kc");
    public static final String DOCKER_IMAGE_MYSQL = System.getProperty("test.docker.image.mysql", "quay.io/debezium/example-mysql:latest");
    public static final String DOCKER_IMAGE_POSTGRESQL = System.getProperty("test.docker.image.postgresql", "quay.io/debezium/example-postgres:latest");
    public static final String DOCKER_IMAGE_MONGO = System.getProperty("test.docker.image.mongo", "quay.io/debezium/example-mongodb:latest");
    public static final String DOCKER_IMAGE_SQLSERVER = System.getProperty("test.docker.image.sqlserver", "mcr.microsoft.com/mssql/server:2019-latest");
    public static final String DOCKER_IMAGE_DB2 = System.getProperty("test.docker.image.db2", "quay.io/debezium/db2-cdc:latest");
    public static final String DOCKER_IMAGE_ORACLE = System.getProperty("test.docker.image.oracle", "quay.io/rh_integration/dbz-oracle:19.3.0");

    // OpenShift configuration
    public static final Optional<String> OCP_URL = stringOptionalProperty("test.ocp.url");
    public static final int OCP_REQUEST_RETRY_BACKOFF_LIMIT = Integer.parseInt(System.getProperty("test.ocp.request.retry.backoff.limit", "5"));
    public static final Optional<String> OCP_USERNAME = stringOptionalProperty("test.ocp.username");
    public static final Optional<String> OCP_PASSWORD = stringOptionalProperty("test.ocp.password");
    public static final String OCP_PROJECT_DBZ = System.getProperty("test.ocp.project.debezium");
    public static final String OCP_PROJECT_REGISTRY = System.getProperty("test.ocp.project.registry", OCP_PROJECT_DBZ + "-registry");
    public static final String OCP_PROJECT_MYSQL = System.getProperty("test.ocp.project.mysql", OCP_PROJECT_DBZ + "-mysql");
    public static final String OCP_PROJECT_POSTGRESQL = System.getProperty("test.ocp.project.postgresql", OCP_PROJECT_DBZ + "-postgresql");
    public static final String OCP_PROJECT_SQLSERVER = System.getProperty("test.ocp.project.sqlserver", OCP_PROJECT_DBZ + "-sqlserver");
    public static final String OCP_PROJECT_MONGO = System.getProperty("test.ocp.project.mongo", OCP_PROJECT_DBZ + "-mongo");
    public static final String OCP_PROJECT_DB2 = System.getProperty("test.ocp.project.db2", OCP_PROJECT_DBZ + "-db2");
    public static final String OCP_PROJECT_ORACLE = System.getProperty("test.ocp.project.oracle", OCP_PROJECT_DBZ + "-oracle");

    public static final Optional<String> OCP_PULL_SECRET_PATH = stringOptionalProperty("test.ocp.pull.secret.paths");

    // Strimzi configuration
    public static final String STRIMZI_OPERATOR_NAME = System.getProperty("test.strimzi.operator.name", "strimzi-cluster-operator");
    public static final boolean STRIMZI_OPERATOR_CONNECTORS = booleanProperty("test.strimzi.operator.connectors", true);
    public static final String STRIMZI_CRD_VERSION = System.getProperty("test.strimzi.crd.version", "v1beta2");
    public static final String STRIMZI_VERSION_KAFKA = System.getProperty("test.strimzi.version.kafka", "3.1.0");
    public static final boolean STRIMZI_KC_BUILD = booleanProperty("test.strimzi.kc.build", true);
    public static final String STRIMZI_KC_IMAGE = System.getProperty("test.strimzi.kc.image");

    // Apicurio Registry configuration
    public static final String APICURIO_LOG_LEVEL = System.getProperty("test.apicurio.log.level", "INFO");

    // MySql Configuration
    public static final String DATABASE_MYSQL_USERNAME = System.getProperty("test.database.mysql.username", "mysqluser");
    public static final String DATABASE_MYSQL_PASSWORD = System.getProperty("test.database.mysql.password", "mysqlpw");
    public static final String DATABASE_MYSQL_DBZ_USERNAME = System.getProperty("test.database.mysql.dbz.username", "debezium");
    public static final String DATABASE_MYSQL_DBZ_PASSWORD = System.getProperty("test.database.mysql.dbz.password", "dbz");
    public static final Optional<String> DATABASE_MYSQL_HOST = stringOptionalProperty("test.database.mysql.host");
    public static final int DATABASE_MYSQL_PORT = Integer.parseInt(System.getProperty("test.database.mysql.port", "3306"));

    // PostgreSql Configuration
    public static final String DATABASE_POSTGRESQL_USERNAME = System.getProperty("test.database.postgresql.username", "debezium");
    public static final String DATABASE_POSTGRESQL_PASSWORD = System.getProperty("test.database.postgresql.password", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_USERNAME = System.getProperty("test.database.postgresql.dbz.username", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_PASSWORD = System.getProperty("test.database.postgresql.dbz.password", "debezium");
    public static final String DATABASE_POSTGRESQL_DBZ_DBNAME = System.getProperty("test.database.postgresql.dbname", "debezium");
    public static final Optional<String> DATABASE_POSTGRESQL_HOST = stringOptionalProperty("test.database.postgresql.host");
    public static final int DATABASE_POSTGRESQL_PORT = Integer.parseInt(System.getProperty("test.database.postgresql.port", "5432"));

    // SqlServer Configuration
    public static final String DATABASE_SQLSERVER_USERNAME = System.getProperty("test.database.sqlserver.username", "sa");
    public static final String DATABASE_SQLSERVER_SA_PASSWORD = System.getProperty("test.database.sqlserver.password", "Debezium1$");
    public static final String DATABASE_SQLSERVER_DBZ_USERNAME = System.getProperty("test.database.sqlserver.dbz.username", DATABASE_SQLSERVER_USERNAME);
    public static final String DATABASE_SQLSERVER_DBZ_PASSWORD = System.getProperty("test.database.sqlserver.dbz.password", DATABASE_SQLSERVER_SA_PASSWORD);
    public static final String DATABASE_SQLSERVER_DBZ_DBNAMES = System.getProperty("test.database.sqlserver.dbnames", "testDB");
    public static final Optional<String> DATABASE_SQLSERVER_HOST = stringOptionalProperty("test.database.sqlserver.host");
    public static final int DATABASE_SQLSERVER_PORT = Integer.parseInt(System.getProperty("test.database.sqlserver.port", "1433"));

    // Mongo Configuration
    public static final String DATABASE_MONGO_USERNAME = System.getProperty("test.database.mongo.username", "admin");
    public static final String DATABASE_MONGO_SA_PASSWORD = System.getProperty("test.database.mongo.password", "admin");
    public static final String DATABASE_MONGO_DBZ_USERNAME = System.getProperty("test.database.mongo.dbz.username", "debezium");
    public static final String DATABASE_MONGO_DBZ_PASSWORD = System.getProperty("test.database.mongo.dbz.password", "dbz");
    public static final String DATABASE_MONGO_DBZ_DBNAME = System.getProperty("test.database.mongo.dbname", "inventory");
    public static final String DATABASE_MONGO_DBZ_LOGIN_DBNAME = System.getProperty("test.database.mongo.dbz.login.dbname", "admin");
    public static final Optional<String> DATABASE_MONGO_HOST = stringOptionalProperty("test.database.mongo.host");
    public static final int DATABASE_MONGO_PORT = Integer.parseInt(System.getProperty("test.database.mongo.port", "27017"));

    // DB2 Configuration
    public static final String DATABASE_DB2_USERNAME = System.getProperty("test.database.db2.username", "db2inst1");
    public static final String DATABASE_DB2_PASSWORD = System.getProperty("test.database.db2.password", "=Password!");
    public static final String DATABASE_DB2_DBZ_USERNAME = System.getProperty("test.database.db2.dbz.username", DATABASE_DB2_USERNAME);
    public static final String DATABASE_DB2_DBZ_PASSWORD = System.getProperty("test.database.db2.dbz.password", DATABASE_DB2_PASSWORD);
    public static final String DATABASE_DB2_DBZ_DBNAME = System.getProperty("test.database.db2.dbname", "TESTDB");
    public static final String DATABASE_DB2_CDC_SCHEMA = System.getProperty("test.database.db2.cdc.schema", "ASNCDC");
    public static final Optional<String> DATABASE_DB2_HOST = stringOptionalProperty("test.database.sqlserver.host");
    public static final int DATABASE_DB2_PORT = Integer.parseInt(System.getProperty("test.database.db2.port", "50000"));

    // Oracle Configuration
    public static final boolean DATABASE_ORACLE = booleanProperty("test.database.oracle", true);
    public static final String DATABASE_ORACLE_USERNAME = System.getProperty("test.database.oracle..username", "debezium");
    public static final String DATABASE_ORACLE_PASSWORD = System.getProperty("test.database.oracle.password", "dbz");
    public static final String DATABASE_ORACLE_DBZ_USERNAME = System.getProperty("test.database.oracle.dbz.username", "c##dbzuser");
    public static final String DATABASE_ORACLE_DBZ_PASSWORD = System.getProperty("test.database.oracle.dbz.password", "dbz");
    public static final String DATABASE_ORACLE_DBNAME = System.getProperty("test.database.oracle.dbname", "ORCLCDB");
    public static final String DATABASE_ORACLE_PDBNAME = System.getProperty("test.database.oracle.pdbname", "ORCLPDB1");

    // Artifact Server
    public static final String ARTIFACT_SERVER_URL = System.getProperty("test.as.url");
    public static final String ARTIFACT_SERVER_DBZ_VERSION = System.getProperty("test.as.debezium.version");
    public static final String ARTIFACT_SERVER_APC_VERSION = System.getProperty("test.as.apicurio.version");
    public static final String ARTIFACT_SERVER_APC_URL = System.getProperty("test.as.apicurio.url");
    public static final String ARTIFACT_SERVER_DB2_DRIVER_VERSION = System.getProperty("test.as.db2.driver.version");
    public static final String ARTIFACT_SERVER_ORACLE_DRIVER_VERSION = System.getProperty("test.as.oracle.driver.version");

    private static boolean booleanProperty(String key) {
        return booleanProperty(key, false);
    }

    private static boolean booleanProperty(String key, boolean defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return !value.isEmpty() && !value.equalsIgnoreCase("false") && !value.equalsIgnoreCase("0");
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
