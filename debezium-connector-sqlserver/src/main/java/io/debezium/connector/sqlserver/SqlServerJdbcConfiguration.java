/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * A specialized JDBC configuration for the Debezium SQL Server driver. This extends the Debezium
 * JDBC driver to include named instance configuration.
 *
 * @author Keri Harris
 */
@Immutable
public interface SqlServerJdbcConfiguration extends JdbcConfiguration {

    /**
     * A field for the named instance of the database server. This field has no default value.
     */
    Field INSTANCE = Field.create("instance",
            "Named instance of the database server");

    /**
     * Obtain a {@link SqlServerJdbcConfiguration} adapter for the given {@link Configuration}.
     *
     * @param config the configuration; may not be null
     * @return the SqlServerJdbcConfiguration; never null
     */
    static SqlServerJdbcConfiguration adapt(Configuration config) {
        if (config instanceof SqlServerJdbcConfiguration) {
            return (SqlServerJdbcConfiguration) config;
        }
        return new SqlServerJdbcConfiguration() {
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
     * The SQL-Server-specific builder used to construct and/or alter JDBC configuration instances.
     *
     * @see SqlServerJdbcConfiguration#copy(Configuration)
     * @see SqlServerJdbcConfiguration#create()
     */
    interface Builder extends Configuration.ConfigBuilder<SqlServerJdbcConfiguration, Builder> {
        /**
         * Use the given named instance in the resulting configuration.
         *
         * @param instance the named instance of the database server
         * @return this builder object so methods can be chained together; never null
         */
        default Builder withInstance(String instance) {
            return with(INSTANCE, instance);
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
            public Builder apply(Consumer<SqlServerJdbcConfiguration.Builder> function) {
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
            public SqlServerJdbcConfiguration build() {
                return SqlServerJdbcConfiguration.adapt(builder.build());
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
            public Builder apply(Consumer<SqlServerJdbcConfiguration.Builder> function) {
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
            public SqlServerJdbcConfiguration build() {
                return SqlServerJdbcConfiguration.adapt(builder.build());
            }

            @Override
            public String toString() {
                return builder.toString();
            }
        };
    }

    /**
     * Get the named instance property from the configuration.
     *
     * @return the specified or default named instance, or null if there is none.
     */
    default String getInstance() {
        return getString(INSTANCE);
    }
}
