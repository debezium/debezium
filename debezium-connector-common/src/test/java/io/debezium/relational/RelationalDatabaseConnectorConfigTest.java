/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;

public class RelationalDatabaseConnectorConfigTest {

    private static final String CONNECTOR = "relational-config-test";
    private static final String ALLOWED_SYSTEM_PROPERTY = CONNECTOR + ".driver.options.allowed";
    private static final String DENIED_SYSTEM_PROPERTY = CONNECTOR + ".driver.options.denied";

    @AfterEach
    public void clearPolicy() {
        System.clearProperty(ALLOWED_SYSTEM_PROPERTY);
        System.clearProperty(DENIED_SYSTEM_PROPERTY);
    }

    @Test
    public void shouldForwardAllDriverOptionsWhenNoPolicyIsSet() {
        final var jdbcConfig = createConfig().getJdbcConfig();

        assertThat(jdbcConfig.getString("connectTimeout")).isEqualTo("5000");
        assertThat(jdbcConfig.getString("sslMode")).isEqualTo("disabled");
    }

    @Test
    public void shouldForwardOnlyAllowedDriverOptionsWhenPolicyIsSet() {
        System.setProperty(ALLOWED_SYSTEM_PROPERTY, "connectTimeout");

        final var jdbcConfig = createConfig().getJdbcConfig();

        assertThat(jdbcConfig.getString("connectTimeout")).isEqualTo("5000");
        assertThat(jdbcConfig.getString("sslMode")).isNull();
        assertThat(jdbcConfig.getHostname()).isEqualTo("localhost");
    }

    @Test
    public void shouldFilterDeniedDriverOptionsWhenPolicyIsSet() {
        System.setProperty(DENIED_SYSTEM_PROPERTY, "sslMode");

        final var jdbcConfig = createConfig().getJdbcConfig();

        assertThat(jdbcConfig.getString("connectTimeout")).isEqualTo("5000");
        assertThat(jdbcConfig.getString("sslMode")).isNull();
        assertThat(jdbcConfig.getHostname()).isEqualTo("localhost");
    }

    @Test
    public void shouldThrowDebeziumExceptionWhenBothAllowedAndDeniedAreSet() {
        System.setProperty(ALLOWED_SYSTEM_PROPERTY, "connectTimeout");
        System.setProperty(DENIED_SYSTEM_PROPERTY, "sslMode");

        assertThatThrownBy(RelationalDatabaseConnectorConfigTest::createConfig)
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Both allowed and denied driver options policies are set for connector 'relational-config-test'");
    }

    private static RelationalDatabaseConnectorConfig createConfig() {
        final var configuration = Configuration.create()
                .with("database.hostname", "localhost")
                .with("driver.connectTimeout", "5000")
                .with("driver.sslMode", "disabled")
                .build();
        return new TestRelationalDatabaseConfig(configuration, null, null, 0) {
            @Override
            public String getConnectorName() {
                return CONNECTOR;
            }
        };
    }
}
