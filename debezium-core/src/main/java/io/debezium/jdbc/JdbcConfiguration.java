/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.time.Duration;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef.Type;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

/**
 * A specialized configuration for the Debezium driver. This defines several known {@link io.debezium.config.Field
 * fields} that are common to all JDBC configurations.
 *
 * @author Randall Hauch
 */
@Immutable
public interface JdbcConfiguration extends Configuration {

    /**
     * A field for the name of the database. This field has no default value.
     */
    Field DATABASE = Field.create("dbname",
            "Name of the database");
    /**
     * A field for the user of the database. This field has no default value.
     */
    Field USER = Field.create("user",
            "Name of the database user to be used when connecting to the database");
    /**
     * A field for the password of the database. This field has no default value.
     */
    Field PASSWORD = Field.create("password",
            "Password to be used when connecting to the database");
    /**
     * A field for the hostname of the database server. This field has no default value.
     */
    Field HOSTNAME = Field.create("hostname", "IP address of the database");

    /**
     * A field for the port of the database server. There is no default value.
     */
    Field PORT = Field.create("port", "Port of the database");

    /**
    * This line will be supplemented with the database connection URL
    */
    Field JDBC_CONNECTION_FLAGS = Field.create("jdbc.connection.flags")
            .withDisplayName("This line will be supplemented with the database connection URL")
            .withType(Type.STRING)
            .withDefault("")
            .withValidation(Field::isOptional);

    /**
     * A semicolon separated list of SQL statements to be executed when the connection to database is established.
     * Typical use-case is setting of session parameters. There is no default value.
     */
    Field ON_CONNECT_STATEMENTS = Field.create("initial.statements", "A semicolon separated list of statements to be executed on connection");

    /**
     * An optional field for datasource factory class that will be used to build the datasource connection pool.
     */
    Field CONNECTION_FACTORY_CLASS = Field.create("connection.factory.class")
            .withDisplayName("Connection factory class")
            .withDescription(
                    "(Incubating) The factory class for creation of datasource connection pool; the FQN of an implementation of io.debezium.jdbc.JdbcConnection.ConnectionFactory must be given.")
            .withType(Type.CLASS)
            .withValidation(Field::isOptional);

    Field CONNECTION_TIMEOUT_MS = Field.create("connection.timeout.ms")
            .withDisplayName("Time to wait for a connection from the pool, given in milliseconds. Defaults to 600 seconds (600,000 ms).")
            .withType(Type.INT)
            .withDefault(600000)
            .withValidation(Field::isOptional);

    Field QUERY_TIMEOUT_MS = Field.create("query.timeout.ms")
            .withDisplayName("Time to wait for a query to execute, given in milliseconds. Defaults to 600 seconds (600,000 ms); zero means there is no limit.")
            .withType(Type.INT)
            .withDefault(600000)
            .withValidation(Field::isOptional);

    /**
     * The set of names of the pre-defined JDBC configuration fields, including {@link #DATABASE}, {@link #USER},
     * {@link #PASSWORD}, {@link #HOSTNAME}, and {@link #PORT}.
     */
    Set<String> ALL_KNOWN_FIELDS = Collect.unmodifiableSet(Field::name, DATABASE, USER, PASSWORD, HOSTNAME, PORT, ON_CONNECT_STATEMENTS,
            JDBC_CONNECTION_FLAGS, CONNECTION_FACTORY_CLASS, CONNECTION_TIMEOUT_MS, QUERY_TIMEOUT_MS);

    /**
     * Obtain a {@link JdbcConfiguration} adapter for the given {@link Configuration}.
     *
     * @param config the configuration; may not be null
     * @return the ClientConfiguration; never null
     */
    static JdbcConfiguration adapt(Configuration config) {
        if (config instanceof JdbcConfiguration) {
            return (JdbcConfiguration) config;
        }
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

    static JdbcConfiguration empty() {
        return JdbcConfiguration.adapt(Configuration.empty());
    }

    /**
     * The JDBC-specific builder used to construct and/or alter JDBC configuration instances.
     *
     * @see JdbcConfiguration#copy(Configuration)
     * @see JdbcConfiguration#create()
     */
    interface Builder extends Configuration.ConfigBuilder<JdbcConfiguration, Builder> {
        /**
         * Use the given user in the resulting configuration.
         *
         * @param username the name of the user
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withUser(String username) {
            return with(USER, username);
        }

        /**
         * Use the given password in the resulting configuration.
         *
         * @param password the password
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withPassword(String password) {
            return with(PASSWORD, password);
        }

        /**
         * Use the given host in the resulting configuration.
         *
         * @param hostname the hostname
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withHostname(String hostname) {
            return with(HOSTNAME, hostname);
        }

        /**
         * Use the given database name in the resulting configuration.
         *
         * @param databaseName the name of the database
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withDatabase(String databaseName) {
            return with(DATABASE, databaseName);
        }

        /**
         * Use the given port in the resulting configuration.
         *
         * @param port the port
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withPort(int port) {
            return with(PORT, port);
        }

        /**
         * Use the given connection factory class in the resulting configuration.
         *
         * @param connectionFactoryClass the connection factory class name
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withConnectionFactoryClass(String connectionFactoryClass) {
            return with(CONNECTION_FACTORY_CLASS, connectionFactoryClass);
        }

        /**
         * Use the given connection factory class in the resulting configuration.
         *
         * @param jdbcConnectionFlags connection string
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withJdbcConnectionFlags(String jdbcConnectionFlags) {
            return with(JDBC_CONNECTION_FLAGS, jdbcConnectionFlags);
        }

        /**
         * Use the given connection timeout in the resulting configuration.
         *
         * @param connectionTimeoutMs connection timeout in ms
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withConnectionTimeoutMs(int connectionTimeoutMs) {
            return with(CONNECTION_TIMEOUT_MS, connectionTimeoutMs);
        }

        /**
         * Use the given query timeout in the resulting configuration.
         *
         * @param queryTimeoutMs query timeout in ms
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withQueryTimeoutMs(int queryTimeoutMs) {
            return with(QUERY_TIMEOUT_MS, queryTimeoutMs);
        }
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     *
     * @param config the configuration to copy
     * @return the configuration builder
     */
    static Builder copy(Configuration config) {
        return new Builder() {
            private Configuration.Builder builder = Configuration.copy(config);

            @Override
            public Builder with(String key, String value) {
                builder.with(key, value);
                return this;
            }

            @Override
            public Builder withDefault(String key, String value) {
                builder.withDefault(key, value);
                return this;
            }

            @Override
            public Builder without(String key) {
                builder.without(key);
                return this;
            }

            @Override
            public Builder apply(Consumer<Builder> function) {
                function.accept(this);
                return this;
            }

            @Override
            public Builder changeString(Field field, Function<String, String> function) {
                changeString(field, function);
                return this;
            }

            @Override
            public Builder changeString(String key, Function<String, String> function) {
                changeString(key, function);
                return this;
            }

            @Override
            public JdbcConfiguration build() {
                return JdbcConfiguration.adapt(builder.build());
            }

            @Override
            public String toString() {
                return builder.toString();
            }
        };
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with an empty configuration.
     *
     * @return the configuration builder
     */
    static Builder create() {
        return new Builder() {
            private Configuration.Builder builder = Configuration.create();

            @Override
            public Builder with(String key, String value) {
                builder.with(key, value);
                return this;
            }

            @Override
            public Builder withDefault(String key, String value) {
                builder.withDefault(key, value);
                return this;
            }

            @Override
            public Builder without(String key) {
                builder.without(key);
                return this;
            }

            @Override
            public Builder apply(Consumer<Builder> function) {
                function.accept(this);
                return this;
            }

            @Override
            public Builder changeString(Field field, Function<String, String> function) {
                changeString(field, function);
                return this;
            }

            @Override
            public Builder changeString(String key, Function<String, String> function) {
                changeString(key, function);
                return this;
            }

            @Override
            public JdbcConfiguration build() {
                return JdbcConfiguration.adapt(builder.build());
            }

            @Override
            public String toString() {
                return builder.toString();
            }
        };
    }

    /**
     * Get a predicate that determines if supplied keys are pre-defined field names.
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
     * Get the hostname property from the configuration.
     *
     * @return the specified or default host name, or null if there is none.
     */
    default String getHostname() {
        return getString(HOSTNAME);
    }

    /**
     * Get the port property from the configuration.
     *
     * @return the specified or default port number, or null if there is none.
     */
    default String getPortAsString() {
        return getString(PORT);
    }

    /**
     * Get the port property from the configuration.
     *
     * @return the specified or default port number, or null if there is none.
     */
    default int getPort() {
        return getInteger(PORT);
    }

    /**
     * Get the database name property from the configuration.
     *
     * @return the specified or default database name, or null if there is none.
     */
    default String getDatabase() {
        return getString(DATABASE);
    }

    /**
     * Get the user property from the configuration.
     *
     * @return the specified or default username, or null if there is none.
     */
    default String getUser() {
        return getString(USER);
    }

    /**
     * Get the password property from the configuration.
     *
     * @return the specified or default password value, or null if there is none.
     */
    default String getPassword() {
        return getString(PASSWORD);
    }

    /**
     * Get the datasource factory property from the configuration.
     *
     * @return the specified value, or null if there is none.
     */
    default String getConnectionFactoryClassName() {
        return getString(CONNECTION_FACTORY_CLASS);
    }

    /**
     * Get the connection string property from the configuration.
     *
     * @return the specified value, or null if there is none.
     */
    default String getJdbcConnectionFlags() {
        return getString(JDBC_CONNECTION_FLAGS);
    }

    /**
     * Get the connection timeout from the configuration.
     *
     * @return the specified value, or null if there is none.
     */
    default Duration getConnectionTimeout() {
        return Duration.ofMillis(getInteger(CONNECTION_TIMEOUT_MS));
    }

    /**
     * Get the query timeout from the configuration.
     *
     * @return the specified value, or null if there is none.
     */
    default Duration getQueryTimeout() {
        return Duration.ofMillis(getInteger(QUERY_TIMEOUT_MS));
    }
}
