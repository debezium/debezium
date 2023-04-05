/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * @author Oren Elias
 */
public class MySqlTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final String USER = "debezium";
    public static final String PASSWORD = "dbz";
    public static final String PRIVILEGED_USER = "mysqluser";
    public static final String PRIVILEGED_PASSWORD = "mysqlpassword";
    public static final String ROOT_PASSWORD = "debezium";
    public static final String DBNAME = "inventory";
    public static final String IMAGE = "quay.io/debezium/example-mysql";
    public static final String HOST = "localhost";
    public static final Integer PORT = 3306;

    private static final GenericContainer<?> container = new GenericContainer<>(IMAGE)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withEnv("MYSQL_USER", PRIVILEGED_USER)
            .withEnv("MYSQL_PASSWORD", PRIVILEGED_PASSWORD)
            .withExposedPorts(PORT)
            .withStartupTimeout(Duration.ofSeconds(180));

    public static GenericContainer<?> getContainer() {
        return container;
    }

    @Override
    public Map<String, String> start() {
        container.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
        params.put("debezium.source.database.hostname", HOST);
        params.put("debezium.source.database.port", container.getMappedPort(PORT).toString());
        params.put("debezium.source.database.user", USER);
        params.put("debezium.source.database.password", PASSWORD);
        params.put("debezium.source.database.dbname", DBNAME);
        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }
}
