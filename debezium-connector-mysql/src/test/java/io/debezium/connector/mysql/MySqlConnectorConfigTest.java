/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;

import org.junit.Test;

import io.debezium.config.Configuration;

public class MySqlConnectorConfigTest {

    @Test
    public void testDefaultTransactionIsolationMode() {
        MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(Configuration.create().build());
        assertThat(mySqlConnectorConfig.getDefaultTransactionIsolationLevel()).isEqualTo(Connection.TRANSACTION_REPEATABLE_READ);
    }
}
