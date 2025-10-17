/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.sqlserver.configuration;

import java.util.function.Supplier;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.recorder.DatasourceRecorder;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class SqlServerDatasourceRecorder implements DatasourceRecorder<SqlServerDatasourceConfiguration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDatasourceRecorder.class);
    public static final String PREFIX = "quarkus.datasource";
    public static final String JDBC_URL = ".jdbc.url";
    public static final String USERNAME = ".username";
    public static final String PASSWORD = ".password";
    public static final String DOT = ".";

    @Override
    public Supplier<SqlServerDatasourceConfiguration> convert(String name, boolean defaultConfiguration) {
        if (defaultConfiguration) {
            LOGGER.trace("Extracting default configuration for data source {}", name);

            String jdbcUrl = ConfigProvider.getConfig().getConfigValue(PREFIX + JDBC_URL).getValue();
            String username = ConfigProvider.getConfig().getConfigValue(PREFIX + USERNAME).getValue();
            String password = ConfigProvider.getConfig().getConfigValue(PREFIX + PASSWORD).getValue();

            return createConfiguration("default", jdbcUrl, username, password, true);
        }
        LOGGER.trace("Extracting datasource configuration for {}", name);

        String jdbcUrl = ConfigProvider.getConfig().getConfigValue(PREFIX + DOT + name + JDBC_URL).getValue();
        String username = ConfigProvider.getConfig().getConfigValue(PREFIX + DOT + name + USERNAME).getValue();
        String password = ConfigProvider.getConfig().getConfigValue(PREFIX + DOT + name + PASSWORD).getValue();

        if (jdbcUrl == null) {
            LOGGER.warn("JDBC URL is null");
            return null;
        }

        return createConfiguration(name, jdbcUrl, username, password, false);
    }

    private Supplier<SqlServerDatasourceConfiguration> createConfiguration(String name,
                                                                           String jdbcUrl,
                                                                           String username,
                                                                           String password,
                                                                           boolean isDefault) {
        return () -> new DatasourceParser(jdbcUrl)
                .asString()
                .map(datasource -> new SqlServerDatasourceConfiguration(
                        datasource.host(),
                        username,
                        password,
                        datasource.database(),
                        datasource.port(),
                        isDefault,
                        name))
                .orElse(null);
    }
}
