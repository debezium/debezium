/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.cube;

import java.util.Map;

import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.Cube;
import org.arquillian.cube.spi.CubeConfiguration;
import org.jboss.arquillian.core.api.Injector;
import org.jboss.arquillian.core.api.InstanceProducer;
import org.jboss.arquillian.core.api.annotation.ApplicationScoped;
import org.jboss.arquillian.core.api.annotation.Inject;
import org.jboss.arquillian.core.api.annotation.Observes;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.cube.AbstractDatabaseCube;
import io.debezium.connector.cube.DatabaseCube;
import io.debezium.connector.cube.DatabaseCubeFactory;
import io.debezium.connector.mysql.MySqlConnectorConfig;

/**
 * Provides an instance of MySQL together with correct connection settings.
 * 
 * @author Jiri Pechanec
 *
 */
public class MySQLDatabaseCubeProducer {
    public static class MySQLDatabaseCube extends AbstractDatabaseCube {

        private static final String ENV_PASSWORD = "MYSQL_PASSWORD";
        private static final String ENV_USER = "MYSQL_USER";
        private static final int PORT = 3306;

        public MySQLDatabaseCube(final Cube<?> cube, final DockerClientExecutor docker) {
            super(cube, docker);
        }

        @Override
        public Builder configuration() {
            final Map<String, String> vars = getEnvVars();
            return Configuration.create()
                    .with(MySqlConnectorConfig.HOSTNAME, getHost())
                    .with(MySqlConnectorConfig.PORT, PORT)
                    .with(MySqlConnectorConfig.USER, vars.get(ENV_USER))
                    .with(MySqlConnectorConfig.PASSWORD, vars.get(ENV_PASSWORD));
        }

        @Override
        public int getPort() {
            return PORT;
        }

    }

    @Inject
    @ApplicationScoped
    private InstanceProducer<DatabaseCubeFactory> databaseCubeProducer;

    public void register(@Observes(precedence = 100) CubeConfiguration configuration, Injector injector) {
        databaseCubeProducer.set(new DatabaseCubeFactory() {
            @Override
            public DatabaseCube createDatabaseCube(Cube<?> cube, DockerClientExecutor docker) {
                return new MySQLDatabaseCube(cube, docker);
            }
        });
    }
}
