/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.util.Collect;

/**
 * A specialized configuration for the Debezium driver.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface JdbcConfiguration extends Configuration {

    public static interface Builder extends Configuration.ConfigBuilder<JdbcConfiguration, Builder> {
        default Builder withUser(String username) {
            return with(Field.USER, username);
        }

        default Builder withPassword(String password) {
            return with(Field.PASSWORD, password);
        }

        default Builder withHostname(String hostname) {
            return with(Field.HOSTNAME, hostname);
        }

        default Builder withDatabase(String databaseName) {
            return with(Field.DATABASE, databaseName);
        }

        default Builder withPort(int port) {
            return with(Field.PORT, port);
        }
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     * 
     * @param config the configuration to copy
     * @return the configuration builder
     */
    public static Builder copy(Configuration config) {
        return new Builder() {
            private Properties props = config.asProperties();

            @Override
            public Builder with(String key, String value) {
                props.setProperty(key, value);
                return this;
            }

            @Override
            public JdbcConfiguration build() {
                return JdbcConfiguration.adapt(Configuration.from(props));
            }

            @Override
            public String toString() {
                return props.toString();
            }
        };
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with an empty configuration.
     * 
     * @return the configuration builder
     */
    public static Builder create() {
        return new Builder() {
            private Properties props = new Properties();

            @Override
            public Builder with(String key, String value) {
                props.setProperty(key, value);
                return this;
            }

            @Override
            public JdbcConfiguration build() {
                return JdbcConfiguration.adapt(Configuration.from(props));
            }

            @Override
            public String toString() {
                return props.toString();
            }
        };
    }

    /**
     * The pre-defined fields for JDBC configurations.
     */
    public static interface Field {
        public static final String USER = "user";
        public static final String PASSWORD = "password";
        public static final String DATABASE = "dbname";
        public static final String HOSTNAME = "hostname";
        public static final String PORT = "port";
    }

    /**
     * The set of pre-defined fields for JDBC configurations.
     */
    public static Set<String> ALL_KNOWN_FIELDS = Collect.unmodifiableSet(Field.DATABASE, Field.USER, Field.PASSWORD, Field.HOSTNAME,
                                                                         Field.PORT);

    /**
     * Get a predicate that determines if supplied keys are {@link Field pre-defined field names}.
     * 
     * @return the predicate; never null
     */
    default Predicate<String> knownFieldNames() {
        return ALL_KNOWN_FIELDS::contains;
    }

    /**
     * Get a view of this configuration that does not contain the {@link #knownFieldNames() known fields}.
     * 
     * @return the filtered view of this configuration; never null
     */
    default Configuration withoutKnownFields() {
        return filter(knownFieldNames().negate());
    }

    /**
     * Obtain a {@link JdbcConfiguration} adapter for the given {@link Configuration}.
     * 
     * @param config the configuration; may not be null
     * @return the ClientConfiguration; never null
     */
    public static JdbcConfiguration adapt(Configuration config) {
        if (config instanceof JdbcConfiguration) return (JdbcConfiguration) config;
        return new JdbcConfiguration() {
            @Override
            public Set<String> keys() {
                return config.keys();
            }

            @Override
            public String getString(String key) {
                return config.getString(key);
            }

            @Override
            public String toString() {
                return config.toString();
            }
        };
    }

    /**
     * Get the hostname property from the configuration.
     * 
     * @return the host name, or null if there is none.
     */
    default String getHostname() {
        return getHostname(null);
    }

    /**
     * Get the hostname property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the host name, or the {@code defaultValue} if there is no such property
     */
    default String getHostname(String defaultValue) {
        return getString(Field.HOSTNAME, defaultValue);
    }

    /**
     * Get the port property from the configuration.
     * 
     * @return the port number, or null if there is none.
     */
    default String getPortAsString() {
        return getPort(null);
    }

    /**
     * Get the port property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the port, or the {@code defaultValue} if there is no such property
     */
    default String getPort(String defaultValue) {
        return getString(Field.PORT, defaultValue);
    }

    /**
     * Get the port property from the configuration.
     * 
     * @return the port number, or null if there is none.
     */
    default Integer getPort() {
        return getInteger(Field.PORT);
    }

    /**
     * Get the port property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the port, or the {@code defaultValue} if there is no such property
     */
    default int getPort(int defaultValue) {
        return getInteger(Field.PORT, defaultValue);
    }

    /**
     * Get the database name property from the configuration.
     * 
     * @return the database name, or null if there is none.
     */
    default String getDatabase() {
        return getDatabase(null);
    }

    /**
     * Get the database name property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the database name, or the {@code defaultValue} if there is no such property
     */
    default String getDatabase(String defaultValue) {
        return getString(Field.DATABASE, defaultValue);
    }

    /**
     * Get the user property from the configuration.
     * 
     * @return the username, or null if there is none.
     */
    default String getUser() {
        return getUser(null);
    }

    /**
     * Get the user property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the username, or the {@code defaultValue} if there is no such property
     */
    default String getUser(String defaultValue) {
        return getString(Field.USER, defaultValue);
    }

    /**
     * Get the password property from the configuration.
     * 
     * @return the password value, or null if there is none.
     */
    default String getPassword() {
        return getPassword(null);
    }

    /**
     * Get the password property from the configuration.
     * 
     * @param defaultValue the value to use by default
     * @return the password, or the {@code defaultValue} if there is no such property
     */
    default String getPassword(String defaultValue) {
        return getString(Field.PASSWORD, defaultValue);
    }
}
