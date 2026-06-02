/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Collect;

/**
 * An Oracle specialization for JDBC configuration.
 *
 * @author Chris Cranford
 */
public interface OracleJdbcConfiguration extends JdbcConfiguration {

    Field URL = Field.create("url", "Connection string url");
    Field STANDBY_URL = Field.create("standby.url", "Connection string url for the Physical Standby");
    Field STANDBY_HOSTNAME = Field.create("standby.hostname", "Hostname of the Physical Standby");
    Field STANDBY_PORT = Field.create("standby.port", "Port of the Physical Standby");

    Set<String> ALL_KNOWN_FIELDS = Collect.unmodifiableSet(
            JdbcConfiguration.ALL_KNOWN_FIELDS,
            Collect.unmodifiableSet(Field::name,
                    URL,
                    STANDBY_URL,
                    STANDBY_HOSTNAME,
                    STANDBY_PORT).toArray(new String[0]));

    interface Builder extends JdbcConfiguration.Builder {
        default Builder withUrl(String url) {
            with(URL, url);
            return this;
        }

        default Builder withStandbyUrl(String standbyUrl) {
            with(STANDBY_URL, standbyUrl);
            return this;
        }

        default Builder withStandbyHostname(String standbyHostname) {
            with(STANDBY_HOSTNAME, standbyHostname);
            return this;
        }

        default Builder withStandbyPort(int standbyPort) {
            with(STANDBY_PORT, standbyPort);
            return this;
        }
    }

    static Builder create() {
        return new OracleJdbcConfigurationBuilder(Configuration.create());
    }

    static Builder copy(Configuration config) {
        return new OracleJdbcConfigurationBuilder(Configuration.copy(config));
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

    default String getStandbyUrl() {
        return getString(STANDBY_URL);
    }

    default String getStandbyHostName() {
        return getString(STANDBY_HOSTNAME);
    }

    default int getStandbyPort() {
        return getInteger(STANDBY_PORT);
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
