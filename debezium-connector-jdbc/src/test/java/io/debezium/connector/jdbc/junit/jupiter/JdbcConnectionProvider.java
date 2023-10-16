/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @author Chris Cranford
 */
public abstract class JdbcConnectionProvider implements AutoCloseable {

    private final JdbcDatabaseContainer<?> container;
    private final ConnectionInitializer initializer;

    private Connection connection;

    public JdbcConnectionProvider(JdbcDatabaseContainer<?> container, ConnectionInitializer initializer) {
        this.container = container;
        this.initializer = initializer;
    }

    public String getUsername() {
        return container.getUsername();
    }

    public String getPassword() {
        return container.getPassword();
    }

    public String getContainerName() {
        return container.getContainerName().substring(1); // starts with slash?
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try {
                // Oracle throws an error, catching it here to allow tests to pass
                connection.close();
            }
            catch (Exception e) {
                // ignoring
                e.printStackTrace();
            }
        }
        connection = null;
    }

    public void execute(String statement) throws SQLException {
        final Connection connection = getConnection();
        try (Statement st = connection.createStatement()) {
            st.execute(statement);
        }
        catch (SQLException e) {
            throw new SQLException("Failed to execute SQL: " + statement, e);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    public void execute(String statement, String sta) throws SQLException {
        final Connection connection = getConnection();
        connection.setAutoCommit(false);

        try (Statement st = connection.createStatement()) {
            st.execute(statement);
            st.execute(sta);
        }
        catch (SQLException e) {
            throw new SQLException("Failed to execute SQL: " + statement, e);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    protected void queryContainer(String header, List<String> commands) throws Exception {
        // CHECKSTYLE:OFF
        System.out.println(header);
        System.out.println("------------------------------------------------------------------------------------------");
        if (commands.size() > 4) {
            final Process proc = Runtime.getRuntime().exec(commands.toArray(new String[]{}));
            final BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            final BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
            proc.waitFor();
            System.out.println("");
        }
        // CHECKSTYLE:ON
    }

    protected JdbcDatabaseContainer<?> getContainer() {
        return container;
    }

    protected Connection getConnection() throws SQLException {
        if (!isInitialized()) {
            connection = container.createConnection("");
            if (initializer != null) {
                initializer.initialize(connection);
            }
        }
        return connection;
    }

    protected boolean isInitialized() throws SQLException {
        return connection != null && !connection.isClosed();
    }

    @FunctionalInterface
    public interface ConnectionInitializer {
        void initialize(Connection connection) throws SQLException;
    }

}
