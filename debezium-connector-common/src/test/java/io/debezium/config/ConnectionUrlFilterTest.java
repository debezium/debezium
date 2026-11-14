/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

public class ConnectionUrlFilterTest {

    private static final String CONNECTOR = "oracle";
    private static final String ALLOWED_SYSTEM_PROPERTY = "oracle.connection.url.allowed";
    private static final String ALLOWED_ENVIRONMENT_VARIABLE = "ORACLE_CONNECTION_URL_ALLOWED";
    private static final String DENIED_SYSTEM_PROPERTY = "oracle.connection.url.denied";
    private static final String DENIED_ENVIRONMENT_VARIABLE = "ORACLE_CONNECTION_URL_DENIED";

    private static final String URL = "jdbc:oracle:thin:@db.example.com:1521/ORCL";
    private static final String OTHER_URL = "jdbc:oracle:thin:@evil.example.org:1521/ORCL";

    @Test
    public void shouldAcceptAnyUrlWhenNoPolicyIsSet() {
        assertThatCode(() -> validate(new Properties(), Map.of(), URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldAcceptAnyUrlWhenConnectorNameIsNull() {
        assertThatCode(() -> new ConnectionUrlFilter(null, Map.of(), new Properties()).validate("database.url", URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldSkipNullAndBlankUrl() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, "jdbc:oracle:thin:@db\\.example\\.com.*");
        assertThatCode(() -> validate(properties, Map.of(), null)).doesNotThrowAnyException();
        assertThatCode(() -> validate(properties, Map.of(), " ")).doesNotThrowAnyException();
    }

    @Test
    public void shouldAcceptUrlMatchingAllowedPattern() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, "jdbc:oracle:thin:@db\\.example\\.com.*");
        assertThatCode(() -> validate(properties, Map.of(), URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldRejectUrlNotMatchingAllowedPattern() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, "jdbc:oracle:thin:@db\\.example\\.com.*");
        assertThatThrownBy(() -> validate(properties, Map.of(), OTHER_URL))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("database.url")
                .hasMessageContaining("not permitted")
                .hasMessageNotContaining(OTHER_URL);
    }

    @Test
    public void shouldRequireAllowedPatternToMatchWholeUrl() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, "jdbc:oracle:thin:@db");
        assertThatThrownBy(() -> validate(properties, Map.of(), URL)).isInstanceOf(DebeziumException.class);
    }

    @Test
    public void shouldRejectUrlMatchingDeniedPattern() {
        final var properties = systemProperties(DENIED_SYSTEM_PROPERTY, ".*evil\\.example\\.org.*");
        assertThatThrownBy(() -> validate(properties, Map.of(), OTHER_URL))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("denied")
                .hasMessageNotContaining(OTHER_URL);
    }

    @Test
    public void shouldAcceptUrlNotMatchingDeniedPattern() {
        final var properties = systemProperties(DENIED_SYSTEM_PROPERTY, ".*evil\\.example\\.org.*");
        assertThatCode(() -> validate(properties, Map.of(), URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldPreferSystemPropertyOverEnvironmentVariable() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, ".*db\\.example\\.com.*");
        assertThatCode(() -> validate(properties, Map.of(ALLOWED_ENVIRONMENT_VARIABLE, "nothing"), URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldUseEnvironmentVariableWhenSystemPropertyIsNotSet() {
        assertThatThrownBy(() -> validate(new Properties(), Map.of(ALLOWED_ENVIRONMENT_VARIABLE, "nothing"), URL))
                .isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> validate(new Properties(), Map.of(DENIED_ENVIRONMENT_VARIABLE, ".*db\\.example\\.com.*"), URL))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    public void shouldIgnoreBlankPolicyValue() {
        assertThatCode(() -> validate(systemProperties(ALLOWED_SYSTEM_PROPERTY, "  "), Map.of(), URL)).doesNotThrowAnyException();
    }

    @Test
    public void shouldFailWhenBothAllowedAndDeniedAreSet() {
        final var properties = systemProperties(ALLOWED_SYSTEM_PROPERTY, ".*");
        assertThatThrownBy(() -> validate(properties, Map.of(DENIED_ENVIRONMENT_VARIABLE, ".*"), URL))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Only one policy may be configured");
    }

    @Test
    public void shouldFailOnInvalidRegularExpression() {
        assertThatThrownBy(() -> validate(systemProperties(ALLOWED_SYSTEM_PROPERTY, "(unclosed"), Map.of(), URL))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid regular expression");
    }

    private static void validate(final Properties properties, final Map<String, String> environment, final String url) {
        new ConnectionUrlFilter(CONNECTOR, environment, properties).validate("database.url", url);
    }

    private static Properties systemProperties(final String name, final String value) {
        final var properties = new Properties();
        properties.setProperty(name, value);
        return properties;
    }
}
