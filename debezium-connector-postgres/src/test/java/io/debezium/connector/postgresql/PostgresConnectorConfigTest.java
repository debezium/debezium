/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.Connection;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.config.Configuration;

public class PostgresConnectorConfigTest {

    @Test
    public void testDefaultTransactionIsolationMode() {
        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(Configuration.create().build());
        Assertions.assertThat(postgresConnectorConfig.getDefaultTransactionIsolationLevel()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
    }
}
