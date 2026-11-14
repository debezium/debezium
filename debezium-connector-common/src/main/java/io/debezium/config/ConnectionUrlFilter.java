/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

/**
 * Restricts the raw connection URL (or connection string) that a connector is allowed to use.
 * <p>
 * The operator sets the policy per connector with either an allow pattern using the system property
 * {@code <connector>.connection.url.allowed} or the environment variable {@code <CONNECTOR>_CONNECTION_URL_ALLOWED},
 * or a deny pattern using the system property {@code <connector>.connection.url.denied} or the environment variable
 * {@code <CONNECTOR>_CONNECTION_URL_DENIED}; system properties take precedence over environment variables.
 * Only one of allow or deny policy may be configured. If both are present, a {@link DebeziumException} is thrown.
 * The value is a Java regular expression that must match the whole URL. With an allow pattern the URL must match,
 * with a deny pattern the URL must not match. When no policy is configured, every URL is accepted.
 * <p>
 * The URL may contain credentials, so it is never included in log messages or exceptions.
 */
public class ConnectionUrlFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUrlFilter.class);

    private static final String SYSTEM_PROPERTY_ALLOWED_SUFFIX = ".connection.url.allowed";
    private static final String ENVIRONMENT_VARIABLE_ALLOWED_SUFFIX = "_CONNECTION_URL_ALLOWED";
    private static final String SYSTEM_PROPERTY_DENIED_SUFFIX = ".connection.url.denied";
    private static final String ENVIRONMENT_VARIABLE_DENIED_SUFFIX = "_CONNECTION_URL_DENIED";

    private record Policy(boolean allowed, Pattern pattern, String source) {
    }

    private final String connectorName;
    private final Policy policy;

    /**
     * Creates a new connection URL filter for the given connector using the system environment and system properties.
     *
     * @param connectorName the connector name, for example {@code oracle}; if {@code null}, no policy applies
     */
    public ConnectionUrlFilter(final String connectorName) {
        this(connectorName, System.getenv(), System.getProperties());
    }

    /**
     * Creates a new connection URL filter for the given connector.
     *
     * @param connectorName the connector name, for example {@code oracle}; if {@code null}, no policy applies
     * @param environment the environment variables, normally {@link System#getenv()}
     * @param systemProperties the system properties, normally {@link System#getProperties()}
     */
    public ConnectionUrlFilter(final String connectorName, final Map<String, String> environment, final Properties systemProperties) {
        this.connectorName = connectorName;
        if (connectorName == null) {
            this.policy = null;
            return;
        }

        final var allowedPolicy = resolvePolicy(true, connectorName + SYSTEM_PROPERTY_ALLOWED_SUFFIX,
                connectorName.toUpperCase(Locale.ROOT) + ENVIRONMENT_VARIABLE_ALLOWED_SUFFIX, environment, systemProperties);
        final var deniedPolicy = resolvePolicy(false, connectorName + SYSTEM_PROPERTY_DENIED_SUFFIX,
                connectorName.toUpperCase(Locale.ROOT) + ENVIRONMENT_VARIABLE_DENIED_SUFFIX, environment, systemProperties);

        if (allowedPolicy != null && deniedPolicy != null) {
            throw new DebeziumException(
                    "Both allowed and denied connection URL policies are set for connector '" + connectorName + "'. Only one policy may be configured.");
        }

        this.policy = allowedPolicy != null ? allowedPolicy : deniedPolicy;
        if (this.policy != null) {
            LOGGER.info("Connection URL policy for connector '{}': {} pattern '{}' (from {})", connectorName,
                    this.policy.allowed() ? "allowed" : "denied", this.policy.pattern().pattern(), this.policy.source());
        }
    }

    /**
     * Verifies that the connection URL satisfies the policy.
     *
     * @param optionName the name of the connector option that holds the URL, used in the error message
     * @param url the connection URL; if {@code null} or blank, the check is skipped
     * @throws DebeziumException if the URL violates the policy
     */
    public void validate(final String optionName, final String url) {
        if (policy == null || Strings.isNullOrBlank(url)) {
            return;
        }
        final var matches = policy.pattern().matcher(url).matches();
        if (policy.allowed() != matches) {
            throw new DebeziumException("The value of '" + optionName + "' is "
                    + (policy.allowed() ? "not permitted" : "denied")
                    + " by the connection URL policy of connector '" + connectorName + "' set via " + policy.source());
        }
    }

    private static Policy resolvePolicy(final boolean allowed, final String systemPropertyName, final String environmentVariableName,
                                        final Map<String, String> environment, final Properties systemProperties) {
        final var systemPropertyValue = systemProperties.getProperty(systemPropertyName);
        if (!Strings.isNullOrBlank(systemPropertyValue)) {
            return new Policy(allowed, compile(systemPropertyValue, systemPropertyName), "system property " + systemPropertyName);
        }
        final var environmentValue = environment.get(environmentVariableName);
        if (!Strings.isNullOrBlank(environmentValue)) {
            return new Policy(allowed, compile(environmentValue, environmentVariableName), "environment variable " + environmentVariableName);
        }
        return null;
    }

    private static Pattern compile(final String regex, final String sourceName) {
        try {
            return Pattern.compile(regex.trim());
        }
        catch (PatternSyntaxException e) {
            throw new DebeziumException("Invalid regular expression in connection URL policy " + sourceName, e);
        }
    }
}
