/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class TestHelper {
    
    protected static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                .withDefault(JdbcConfiguration.DATABASE, "master")
                                .withDefault(JdbcConfiguration.HOSTNAME, "192.168.99.100")
                                .withDefault(JdbcConfiguration.PORT, 1433)
                                .withDefault(JdbcConfiguration.USER, "sa")
                                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                                .build();
    }
}
