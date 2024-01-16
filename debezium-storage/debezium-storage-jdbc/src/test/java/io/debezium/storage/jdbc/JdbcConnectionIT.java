/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.debezium.embedded.AbstractConnectorTest;

/**
 * JDBC Connection Integration Test.
 *
 * @author Ilyas Ahsan
 */
public class JdbcConnectionIT extends AbstractConnectorTest {

    private static final String PRIVILEGED_USER = "mysqluser";
    private static final String INVALID_USER = "invalidUser";
    private static final String PRIVILEGED_PASSWORD = "mysqlpassword";
    private static final String ROOT_PASSWORD = "debezium";
    private static final String DBNAME = "inventory";
    private static final String IMAGE = "debezium/example-mysql";
    private static final Integer PORT = 3306;
    private static final Duration WAIT_RETRY_DELAY = Duration.ofMillis(1000L);
    private static final Integer MAX_RETRY_COUNT = 10;
    private static final GenericContainer<?> container = new GenericContainer<>(IMAGE)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withEnv("MYSQL_USER", PRIVILEGED_USER)
            .withEnv("MYSQL_PASSWORD", PRIVILEGED_PASSWORD)
            .withExposedPorts(PORT)
            .withStartupTimeout(Duration.ofSeconds(180));

    private JdbcConnection jdbcConnection;

    @BeforeClass
    public static void startDatabase() {
        container.start();
    }

    @AfterClass
    public static void stopDatabase() {
        container.stop();
    }

    @Before
    public void beforeEach() {
        stopConnector();
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    @Test
    public void shouldConnected() {
        AtomicBoolean isValid = new AtomicBoolean(false);
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", container.getHost(), container.getMappedPort(PORT), DBNAME);
        jdbcConnection = new JdbcConnection(jdbcUrl, PRIVILEGED_USER, PRIVILEGED_PASSWORD, WAIT_RETRY_DELAY, MAX_RETRY_COUNT);
        jdbcConnection.execute(conn -> {
            isValid.set(conn.isValid(1));
        }, false);
        assertTrue(isValid.get());
    }

    @Test(expected = Exception.class)
    public void shouldConnectionFail() {
        AtomicBoolean isValid = new AtomicBoolean(false);
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", container.getHost(), container.getMappedPort(PORT), DBNAME);
        jdbcConnection = new JdbcConnection(jdbcUrl, INVALID_USER, PRIVILEGED_PASSWORD, WAIT_RETRY_DELAY, MAX_RETRY_COUNT);
        jdbcConnection.execute(conn -> {
            isValid.set(conn.isValid(1));
        }, false);
        assertFalse(isValid.get());
    }

    @Test
    public void shouldRetryConnectionBecauseSQLRecoverableException() {
        AtomicBoolean isValid = new AtomicBoolean(true);
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", container.getHost(), container.getMappedPort(PORT), DBNAME);
        jdbcConnection = new JdbcConnection(jdbcUrl, PRIVILEGED_USER, PRIVILEGED_PASSWORD, WAIT_RETRY_DELAY, MAX_RETRY_COUNT);
        try {
            jdbcConnection.execute(conn -> {
                conn.close();
                throw new SQLRecoverableException("SQLRecoverableException");
            }, false);
        }
        catch (Exception e) {
            isValid.set(false);
        }
        assertTrue(isValid.get());
    }
}
