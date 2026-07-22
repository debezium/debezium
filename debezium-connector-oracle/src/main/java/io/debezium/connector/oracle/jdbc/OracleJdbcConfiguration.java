/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import static io.debezium.config.ConfigurationNames.DRIVER_CONFIG_PREFIX;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Collect;

/**
 * An Oracle specialization for JDBC configuration.
 *
 * @author Chris Cranford
 */
public interface OracleJdbcConfiguration extends JdbcConfiguration {

    String SECONDARY_PREFIX = "secondary.";

    Field URL = Field.create("url", "Connection string url");

    Field SECONDARY_URL = Field.create(SECONDARY_PREFIX + URL.name(), "Connection string url for the Secondary Oracle instance");
    Field SECONDARY_HOSTNAME = Field.create(SECONDARY_PREFIX + HOSTNAME.name(), "Hostname of the Secondary Oracle instance");
    Field SECONDARY_PORT = Field.create(SECONDARY_PREFIX + PORT.name(), "Port of the Secondary Oracle instance");
    Field SECONDARY_DATABASE = Field.create(SECONDARY_PREFIX + DATABASE.name(), "Name of the Secondary Oracle database");

    Set<String> ALL_KNOWN_FIELDS = Collect.unmodifiableSet(
            JdbcConfiguration.ALL_KNOWN_FIELDS,
            Collect.unmodifiableSet(Field::name,
                    URL,
                    SECONDARY_URL,
                    SECONDARY_HOSTNAME,
                    SECONDARY_PORT,
                    SECONDARY_DATABASE).toArray(new String[0]));

    interface Builder extends JdbcConfiguration.Builder {
        default Builder withUrl(String url) {
            with(URL, url);
            return this;
        }

        default Builder withSecondaryUrl(String standbyUrl) {
            with(SECONDARY_URL, standbyUrl);
            return this;
        }

        default Builder withSecondaryHostname(String standbyHostname) {
            with(SECONDARY_HOSTNAME, standbyHostname);
            return this;
        }

        default Builder withSecondaryPort(int standbyPort) {
            with(SECONDARY_PORT, standbyPort);
            return this;
        }

        default Builder withSecondaryDatabaseName(String databaseName) {
            with(SECONDARY_DATABASE, databaseName);
            return this;
        }
    }

    static Builder create() {
        return new OracleJdbcConfigurationBuilder(Configuration.create());
    }

    static Builder copy(Configuration config) {
        return new OracleJdbcConfigurationBuilder(Configuration.copy(config));
    }

    static OracleJdbcConfiguration adaptWithSubset(Configuration config) {
        return OracleJdbcConfiguration.adapt(config.subset(ConfigurationNames.DATABASE_CONFIG_PREFIX, true)
                .merge(config.subset(DRIVER_CONFIG_PREFIX, true))
                .merge(config.subset(OracleJdbcConfiguration.SECONDARY_PREFIX, false)));
    }

    static OracleJdbcConfiguration adapt(Configuration config) {
        if (config instanceof OracleJdbcConfiguration jdbcConfig) {
            return jdbcConfig;
        }

        return new OracleJdbcConfiguration() {
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

    @Override
    default Predicate<String> knownFieldNames() {
        return ALL_KNOWN_FIELDS::contains;
    }

    default String getUrl() {
        return getString(URL);
    }

    default String getSecondaryUrl() {
        return getString(SECONDARY_URL);
    }

    default String getSecondaryHostname() {
        return getString(SECONDARY_HOSTNAME);
    }

    default int getSecondaryPort() {
        // When set to 0, will fall back to using PORT
        return hasKey(SECONDARY_PORT) ? getInteger(SECONDARY_PORT) : 0;
    }

    default String getSecondaryDatabaseName() {
        return getString(SECONDARY_DATABASE);
    }

    class OracleJdbcConfigurationBuilder implements Builder {

        private final Configuration.ConfigBuilder<Configuration, ?> builder;

        OracleJdbcConfigurationBuilder(Configuration.ConfigBuilder<Configuration, ?> builder) {
            this.builder = builder;
        }

        @Override
        public JdbcConfiguration.Builder with(String key, String value) {
            builder.with(key, value);
            return this;
        }

        @Override
        public JdbcConfiguration.Builder withDefault(String key, String value) {
            builder.withDefault(key, value);
            return this;
        }

        @Override
        public JdbcConfiguration.Builder without(String key) {
            builder.without(key);
            return this;
        }

        @Override
        public JdbcConfiguration.Builder apply(Consumer<JdbcConfiguration.Builder> function) {
            function.accept(this);
            return this;
        }

        @Override
        public JdbcConfiguration.Builder changeString(Field field, Function<String, String> function) {
            builder.changeString(field, function);
            return this;
        }

        @Override
        public JdbcConfiguration.Builder changeString(String key, Function<String, String> function) {
            builder.changeString(key, function);
            return this;
        }

        @Override
        public OracleJdbcConfiguration build() {
            return OracleJdbcConfiguration.adapt(builder.build());
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
