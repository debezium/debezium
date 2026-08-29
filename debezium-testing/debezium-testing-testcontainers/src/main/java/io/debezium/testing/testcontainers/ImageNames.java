/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.utility.DockerImageName;

public final class ImageNames {

    private static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:15";

    private static final String TIMESCALE_DB_IMAGE = "quay.io/debezium/timescale-timescaledb:latest-pg15";

    private static final String SINGLESTORE_IMAGE = "ghcr.io/singlestore-labs/singlestoredb-dev:0.2.77";

    // The explicit registry prevents the image name from being rewritten by the
    // hub.image.name.prefix substitution; the image is only published on Docker Hub.
    private static final String STARROCKS_IMAGE = "docker.io/starrocks/allin1-ubuntu:4.1.1";

    public static final String OFFICIAL_MONGODB_IMAGE = "quay.io/debezium/official-mongo:8.0";

    public static final String MYSQL_EXAMPLE_IMAGE = "quay.io/debezium/example-mysql";

    public static final String SQLSERVER_22_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";

    public static final String MYSQL_9_IMAGE = "container-registry.oracle.com/mysql/community-server:9.0";

    public static final String POSTGRES_EXAMPLE_IMAGE = "quay.io/debezium/example-postgres";

    public static final String MYSQL_LATEST_IMAGE = "quay.io/debezium/example-mysql-master:latest";

    public static final String MYSQL_REPLICA_IMAGE = "quay.io/debezium/example-mysql-replica:latest";

    public static final String POSTGRES_EXAMPLE_LATEST_IMAGE = "quay.io/debezium/example-postgres:latest";

    public static final String MONGO_EXAMPLE_IMAGE = "quay.io/debezium/example-mongodb:2.6";

    public static final String SQLSERVER_2019_IMAGE = "mcr.microsoft.com/mssql/server:2019-latest";

    public static final String DB2_IMAGE = "quay.io/debezium/db2-cdc:latest";

    public static final String ORACLE_DBZ_19_3_0_IMAGE = "quay.io/rh_integration/dbz-oracle:19.3.0";

    public static final String INFORMIX_IMAGE = "quay.io/rh_integration/dbz-informix:14";

    public static final String DEBEZIUM_IMAGE = "quay.io/debezium/connect";

    public static final String SCHEMA_REGISTERY_IMAGE = "quay.io/debezium/confluentinc-cp-schema-registry:6.0.2";

    public static final String APICURIO_IMAGE = "quay.io/apicurio/apicurio-registry";

    public static final String REDIS_ALPINE_IMAGE = "redis:5.0.3-alpine";

    public static final String MYSQL_9_7_IMAGE = "container-registry.oracle.com/mysql/community-server:9.7";

    public static final String DOCLING_1_15_IMAGE = "quay.io/docling-project/docling-serve:v1.15.0";

    public static final String OLLAMA_0_6_2_IMAGE = "mirror.gcr.io/ollama/ollama:0.6.2";

    public static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite";

    public static final String MONGO_OFFICIAL_IMAGE = "quay.io/debezium/official-mongo";

    public static final String DBZ_ORACLE_IMAGE = "quay.io/rh_integration/dbz-oracle";

    public static final String MARIADB_IMAGE = "quay.io/debezium/example-mariadb";

    public static final String KAFKA_IMAGE = "quay.io/debezium/confluentinc-cp-kafka:7.2.10";

    public static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName TIMESCALE_DB_IMAGE_NAME = DockerImageName.parse(TIMESCALE_DB_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName SINGLESTORE_DOCKER_IMAGE_NAME = DockerImageName.parse(SINGLESTORE_IMAGE);

    public static final DockerImageName STARROCKS_DOCKER_IMAGE_NAME = DockerImageName.parse(STARROCKS_IMAGE);

    public static final DockerImageName OFFICIAL_DOCKER_IMAGE_NAME = DockerImageName.parse(OFFICIAL_MONGODB_IMAGE)
            .asCompatibleSubstituteFor("mongodb");

    public static final DockerImageName MYSQL_EXAMPLE_IMAGE_NAME = DockerImageName.parse(MYSQL_EXAMPLE_IMAGE);

    public static final DockerImageName SQLSERVER_22_IMAGE_NAME = DockerImageName.parse(SQLSERVER_22_IMAGE);

    public static final DockerImageName MYSQL_9_IMAGE_NAME = DockerImageName.parse(MYSQL_9_IMAGE);

    public static final DockerImageName POSTGRES_EXAMPLE_IMAGE_NAME = DockerImageName.parse(POSTGRES_EXAMPLE_IMAGE);

    public static final DockerImageName MYSQL_LATEST_IMAGE_NAME = DockerImageName.parse(MYSQL_LATEST_IMAGE);

    public static final DockerImageName MYSQL_REPLICA_IMAGE_NAME = DockerImageName.parse(MYSQL_REPLICA_IMAGE);

    public static final DockerImageName POSTGRES_EXAMPLE_LATEST_IMAGE_NAME = DockerImageName.parse(POSTGRES_EXAMPLE_LATEST_IMAGE);

    public static final DockerImageName MONGO_EXAMPLE_IMAGE_NAME = DockerImageName.parse(MONGO_EXAMPLE_IMAGE);

    public static final DockerImageName SQLSERVER_2019_IMAGE_NAME = DockerImageName.parse(SQLSERVER_2019_IMAGE);

    public static final DockerImageName DB2_IMAGE_NAME = DockerImageName.parse(DB2_IMAGE)
            .asCompatibleSubstituteFor("ibmcom/db2");

    public static final DockerImageName ORACLE_DBZ_19_3_0_IMAGE_NAME = DockerImageName.parse(ORACLE_DBZ_19_3_0_IMAGE);

    public static final DockerImageName INFORMIX_IMAGE_NAME = DockerImageName.parse(INFORMIX_IMAGE);

    public static final DockerImageName APICURIO_IMAGE_NAME = DockerImageName.parse(APICURIO_IMAGE);

    public static final DockerImageName REDIS_ALPINE_IMAGE_NAME = DockerImageName.parse(REDIS_ALPINE_IMAGE);

    public static final DockerImageName MYSQL_9_7_IMAGE_NAME = DockerImageName.parse(MYSQL_9_7_IMAGE);

    public static final DockerImageName DOCLING_1_15_IMAGE_NAME = DockerImageName.parse(DOCLING_1_15_IMAGE);

    public static final DockerImageName OLLAMA_0_6_2_IMAGE_NAME = DockerImageName.parse(OLLAMA_0_6_2_IMAGE);

    public static final DockerImageName DEBEZIUM_IMAGE_NAME = DockerImageName.parse(DEBEZIUM_IMAGE);

    public static final DockerImageName SCHEMA_REGISTERY_IMAGE_NAME = DockerImageName.parse(SCHEMA_REGISTERY_IMAGE);

    public static final DockerImageName DB2_11_5_9_IMAGE_NAME = DockerImageName.parse("icr.io/db2_community/db2:11.5.9.0");

    public static final DockerImageName COCKROACHDB_25_4_12_IMAGE_NAME = DockerImageName.parse("docker.io/cockroachdb/cockroach:v25.4.12")
            .asCompatibleSubstituteFor("cockroachdb/cockroach");

    public static final DockerImageName POSTGRES_17_IMAGE_NAME = DockerImageName.parse("quay.io/debezium/postgres:17")
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName AZURITE_IMAGE_NAME = DockerImageName.parse(AZURITE_IMAGE);

    public static final DockerImageName MONGO_OFFICIAL_IMAGE_NAME = DockerImageName.parse(MONGO_OFFICIAL_IMAGE);

    public static final DockerImageName DBZ_ORACLE_IMAGE_NAME = DockerImageName.parse(DBZ_ORACLE_IMAGE);

    public static final DockerImageName MARIADB_IMAGE_NAME = DockerImageName.parse(MARIADB_IMAGE);

    public static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName.parse(KAFKA_IMAGE);
}
