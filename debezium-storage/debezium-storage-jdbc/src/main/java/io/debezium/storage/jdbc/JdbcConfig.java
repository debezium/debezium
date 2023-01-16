/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;

import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.*;

public class JdbcConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    static {
        CONFIG = WorkerConfig.baseConfigDef()
                .define(JDBC_URI.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database uri")
                .define(JDBC_USER.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database username")
                .define(JDBC_PASSWORD.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database password")
                .define(OFFSET_STORAGE_TABLE_NAME.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Name of the table to store offsets");
    }

    public JdbcConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
