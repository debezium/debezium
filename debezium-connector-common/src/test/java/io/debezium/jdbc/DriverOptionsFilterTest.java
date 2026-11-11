/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class DriverOptionsFilterTest {

    private static final String CONNECTOR = "mysql";
    private static final String SYSTEM_PROPERTY = "mysql.driver.options.allowed";
    private static final String ENVIRONMENT_VARIABLE = "MYSQL_DRIVER_OPTIONS_ALLOWED";

    private static final Configuration DRIVER_OPTIONS = Configuration.create()
            .with("sslMode", "required")
            .with("connectTimeout", "5000")
            .with("useSSL", "true")
            .build();

    @Test
    public void shouldAllowAllOptionsWhenNoPolicyIsSet() {
        assertThat(filter(new Properties(), Map.of()).keys()).containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldAllowAllOptionsWhenPolicyIsAll() {
        assertThat(filter(systemProperties("ALL"), Map.of()).keys()).containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldAllowNoOptionsWhenPolicyIsNone() {
        assertThat(filter(systemProperties("NONE"), Map.of()).keys()).isEmpty();
    }

    @Test
    public void shouldAllowOnlyListedOptionsWithTheirValues() {
        final var allowed = filter(systemProperties("sslMode, connectTimeout"), Map.of());

        assertThat(allowed.keys()).containsExactlyInAnyOrder("sslMode", "connectTimeout");
        assertThat(allowed.getString("sslMode")).isEqualTo("required");
    }

    @Test
    public void shouldTreatKeywordsCaseInsensitively() {
        assertThat(filter(systemProperties("none"), Map.of()).keys()).isEmpty();
        assertThat(filter(systemProperties("all"), Map.of()).keys()).containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldMatchOptionNamesCaseSensitively() {
        assertThat(filter(systemProperties("SSLMODE"), Map.of()).keys()).isEmpty();
    }

    @Test
    public void shouldIgnoreEmptyEntriesInList() {
        assertThat(filter(systemProperties(" ,sslMode,, "), Map.of()).keys()).containsExactly("sslMode");
    }

    @Test
    public void shouldPreferSystemPropertyOverEnvironmentVariable() {
        assertThat(filter(systemProperties("NONE"), Map.of(ENVIRONMENT_VARIABLE, "ALL")).keys()).isEmpty();
    }

    @Test
    public void shouldUseEnvironmentVariableWhenSystemPropertyIsNotSet() {
        assertThat(filter(new Properties(), Map.of(ENVIRONMENT_VARIABLE, "useSSL")).keys()).containsExactly("useSSL");
    }

    @Test
    public void shouldIgnoreBlankSettings() {
        assertThat(filter(systemProperties(" "), Map.of(ENVIRONMENT_VARIABLE, "")).keys())
                .containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldApplyPolicyOnlyToTheNamedConnector() {
        final var properties = new Properties();
        properties.setProperty("postgresql.driver.options.allowed", "NONE");

        assertThat(filter(properties, Map.of("POSTGRESQL_DRIVER_OPTIONS_ALLOWED", "NONE")).keys())
                .containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldAllowAllOptionsWhenConnectorHasNoName() {
        assertThat(new DriverOptionsFilter(null, Map.of(), systemProperties("NONE")).filter(DRIVER_OPTIONS).keys())
                .containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    @Test
    public void shouldFilterOptionsUsingDefaultEnvironmentAndSystemProperties() {
        assertThat(new DriverOptionsFilter(CONNECTOR).filter(DRIVER_OPTIONS).keys())
                .containsExactlyInAnyOrder("sslMode", "connectTimeout", "useSSL");
    }

    private static Configuration filter(final Properties properties, final Map<String, String> environment) {
        return new DriverOptionsFilter(CONNECTOR, environment, properties).filter(DRIVER_OPTIONS);
    }

    private static Properties systemProperties(final String policy) {
        final var properties = new Properties();
        properties.setProperty(SYSTEM_PROPERTY, policy);
        return properties;
    }
}
