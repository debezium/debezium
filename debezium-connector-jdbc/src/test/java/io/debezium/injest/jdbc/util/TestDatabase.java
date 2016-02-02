/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.injest.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDatabase {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestDatabase.class);

    public static String hostname() {
        return hostname(null);
    }

    public static String hostname(String defaultValue) {
        return System.getProperty("database.hostname", defaultValue);
    }

    public static String port() {
        return port(null);
    }

    public static String port(String defaultValue) {
        return System.getProperty("database.port", defaultValue);
    }

    public static String username() {
        return username(null);
    }

    public static String username(String defaultValue) {
        return System.getProperty("database.username", defaultValue);
    }

    public static String password() {
        return password(null);
    }

    public static String password(String defaultValue) {
        return System.getProperty("database.password", defaultValue);
    }

    public static Connection connect(String urlPattern) throws SQLException {
        return connect(urlPattern,null);
    }

    public static Connection connect(String urlPattern, Consumer<Properties> setProperties) throws SQLException {
        return connect(env -> {
            return urlPattern.replaceAll("\\$\\{hostname\\}", hostname("localhost"))
                             .replaceAll("\\$\\{port\\}", port())
                             .replaceAll("\\$\\{username\\}", username("")) // removes if there is no username
                             .replaceAll("\\$\\{password\\}", password("")); // removes if there is no password
        } , setProperties);
    }

    public static Connection connect(Function<Environment, String> urlBuilder) throws SQLException {
        return connect(urlBuilder,null);
    }

    public static Connection connect(Function<Environment, String> urlBuilder, Consumer<Properties> setProperties) throws SQLException {
        Environment env = new Environment() {
            @Override
            public String hostname(String defaultValue) {
                return TestDatabase.hostname(defaultValue);
            }

            @Override
            public String password(String defaultValue) {
                return TestDatabase.password(defaultValue);
            }

            @Override
            public String port(String defaultValue) {
                return TestDatabase.port(defaultValue);
            }

            @Override
            public String username(String defaultValue) {
                return TestDatabase.username(defaultValue);
            }
        };
        String url = urlBuilder.apply(env);
        Properties props = new Properties();
        String username = username();
        if (username != null) {
            props.setProperty("user", username);
        }
        String password = password();
        if (password != null) {
            props.setProperty("password", password);
        }
        if (setProperties != null) {
            setProperties.accept(props);
        }
        Connection conn = DriverManager.getConnection(url, props);
        LOGGER.info("Connected to {} with {}",url, props);
        return conn;
    }

    public static interface Environment {
        default String username() {
            return username(null);
        }

        default String password() {
            return password(null);
        }

        default String hostname() {
            return hostname(null);
        }

        default String port() {
            return port(null);
        }

        String username(String defaultValue);

        String password(String defaultValue);

        String hostname(String defaultValue);

        String port(String defaultValue);

        default String property(String name) {
            return System.getProperty(name);
        }

        default String property(String name, String defaultValue) {
            return System.getProperty(name, defaultValue);
        }
    }

    public TestDatabase() {
    }

}
