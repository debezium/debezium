/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.docker;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DBZPostgreSQLContainer<SELF extends DBZPostgreSQLContainer<SELF>>
        extends PostgreSQLContainer<SELF> {

    private boolean existingDatabase = false;

    public DBZPostgreSQLContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName).asCompatibleSubstituteFor("postgres"));
    }

    public DBZPostgreSQLContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public SELF withExistingDatabase(String dbName) {
        this.existingDatabase = true;
        return withDatabaseName(dbName);
    }

    @Override
    protected void configure() {
        super.configure();
        if (existingDatabase) {
            getEnvMap().remove("POSTGRES_DB");
        }
    }
}
