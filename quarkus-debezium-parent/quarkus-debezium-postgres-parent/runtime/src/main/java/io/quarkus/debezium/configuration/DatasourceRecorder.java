/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import java.util.function.Supplier;

import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DatasourceRecorder {

    public static final String PREFIX = "quarkus.datasource.";
    public static final String JDBC_URL = ".jdbc.url";
    public static final String USERNAME = ".username";
    public static final String PASSWORD = ".password";

    public Supplier<PostgresDatasourceConfiguration> convert(String name, boolean defaultConfiguration) {
        if (defaultConfiguration) {
            String jdbcUrl = ConfigProvider.getConfig().getConfigValue(PREFIX + JDBC_URL).getValue();
            String username = ConfigProvider.getConfig().getConfigValue(PREFIX + USERNAME).getValue();
            String password = ConfigProvider.getConfig().getConfigValue(PREFIX + PASSWORD).getValue();

            return createConfiguration(name, jdbcUrl, username, password, true);
        }

        String jdbcUrl = ConfigProvider.getConfig().getConfigValue(PREFIX + name + JDBC_URL).getValue();
        String username = ConfigProvider.getConfig().getConfigValue(PREFIX + name + USERNAME).getValue();
        String password = ConfigProvider.getConfig().getConfigValue(PREFIX + name + PASSWORD).getValue();

        if (jdbcUrl == null) {
            return null;
        }

        return createConfiguration(name, jdbcUrl, username, password, false);
    }

    private Supplier<PostgresDatasourceConfiguration> createConfiguration(String name,
                                                                          String jdbcUrl,
                                                                          String username,
                                                                          String password,
                                                                          boolean isDefault) {
        return () -> new DatasourceParser(jdbcUrl)
                .asString()
                .map(datasource -> new PostgresDatasourceConfiguration(
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
