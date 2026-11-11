/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.util.Strings;

/**
 * Restricts the driver pass-through options ({@code driver.*}) that are forwarded to the JDBC driver.
 * <p>
 * The operator sets the policy per connector with either an allow list using the system property
 * {@code <connector>.driver.options.allowed} or the environment variable {@code <CONNECTOR>_DRIVER_OPTIONS_ALLOWED},
 * or a deny list using the system property {@code <connector>.driver.options.denied} or the environment variable
 * {@code <CONNECTOR>_DRIVER_OPTIONS_DENIED}; system properties take precedence over environment variables.
 * Only one of allow or deny policy may be configured. If both are present, a {@link DebeziumException} is thrown.
 * The value is {@code ALL}, {@code NONE}, or a comma-separated list of the driver option names, for example
 * {@code sslMode,connectTimeout}. When no policy is configured, the default is allow {@code ALL}.
 */
public class DriverOptionsFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverOptionsFilter.class);

    private static final String ALL = "ALL";
    private static final String NONE = "NONE";
    private static final String SYSTEM_PROPERTY_ALLOWED_SUFFIX = ".driver.options.allowed";
    private static final String ENVIRONMENT_VARIABLE_ALLOWED_SUFFIX = "_DRIVER_OPTIONS_ALLOWED";
    private static final String SYSTEM_PROPERTY_DENIED_SUFFIX = ".driver.options.denied";
    private static final String ENVIRONMENT_VARIABLE_DENIED_SUFFIX = "_DRIVER_OPTIONS_DENIED";

    private enum Mode {
        ALLOWED,
        DENIED
    }

    private record Policy(Mode mode, String setting, String source) {
    }

    private final String connectorName;
    private final Mode mode;
    private final String setting;
    private final Set<String> options;

    /**
     * Creates a new driver options filter for the given connector using the system environment and system properties.
     *
     * @param connectorName the connector name, for example {@code mysql}; if {@code null}, no policy applies
     */
    public DriverOptionsFilter(final String connectorName) {
        this(connectorName, System.getenv(), System.getProperties());
    }

    /**
     * Creates a new driver options filter for the given connector.
     *
     * @param connectorName the connector name, for example {@code mysql}; if {@code null}, no policy applies
     * @param environment the environment variables, normally {@link System#getenv()}
     * @param systemProperties the system properties, normally {@link System#getProperties()}
     */
    public DriverOptionsFilter(final String connectorName, final Map<String, String> environment, final Properties systemProperties) {
        this.connectorName = connectorName;
        if (connectorName == null) {
            this.mode = Mode.ALLOWED;
            this.setting = ALL;
            this.options = Set.of();
            return;
        }

        final var allowedSystemPropertyName = connectorName + SYSTEM_PROPERTY_ALLOWED_SUFFIX;
        final var allowedEnvironmentVariableName = connectorName.toUpperCase(Locale.ROOT) + ENVIRONMENT_VARIABLE_ALLOWED_SUFFIX;
        final var allowedPolicy = resolvePolicy(Mode.ALLOWED, allowedSystemPropertyName, allowedEnvironmentVariableName, environment, systemProperties);

        final var deniedSystemPropertyName = connectorName + SYSTEM_PROPERTY_DENIED_SUFFIX;
        final var deniedEnvironmentVariableName = connectorName.toUpperCase(Locale.ROOT) + ENVIRONMENT_VARIABLE_DENIED_SUFFIX;
        final var deniedPolicy = resolvePolicy(Mode.DENIED, deniedSystemPropertyName, deniedEnvironmentVariableName, environment, systemProperties);

        if (allowedPolicy != null && deniedPolicy != null) {
            throw new DebeziumException(
                    "Both allowed and denied driver options policies are set for connector '" + connectorName + "'. Only one policy may be configured.");
        }

        final var policy = allowedPolicy != null
                ? allowedPolicy
                : (deniedPolicy != null ? deniedPolicy : new Policy(Mode.ALLOWED, ALL, "default"));

        this.mode = policy.mode();
        this.setting = policy.setting();

        LOGGER.info("Driver pass-through options policy for connector '{}': {} (from {})", connectorName, this.setting, policy.source());

        if (ALL.equalsIgnoreCase(this.setting) || NONE.equalsIgnoreCase(this.setting)) {
            this.options = Set.of();
        }
        else {
            this.options = parseOptionNames(this.setting);
        }
    }

    /**
     * Returns the driver options that the policy of this connector allows.
     *
     * @param driverOptions the driver options with the {@code driver.} prefix removed
     * @return the driver options allowed by the policy; never null
     */
    public Configuration filter(final Configuration driverOptions) {
        if (connectorName == null || (mode == Mode.ALLOWED && ALL.equalsIgnoreCase(setting)) || (mode == Mode.DENIED && NONE.equalsIgnoreCase(setting))) {
            return driverOptions;
        }

        final Set<String> filteredOut = driverOptions.keys().stream()
                .filter(name -> !isAllowed(name))
                .collect(Collectors.toCollection(TreeSet::new));
        if (!filteredOut.isEmpty()) {
            LOGGER.warn("Driver pass-through options filtered out for connector '{}' by the policy: {}", connectorName, filteredOut);
        }
        return driverOptions.filter(this::isAllowed);
    }

    private boolean isAllowed(final String name) {
        if (connectorName == null) {
            return true;
        }
        return switch (mode) {
            case ALLOWED -> {
                if (ALL.equalsIgnoreCase(setting)) {
                    yield true;
                }
                if (NONE.equalsIgnoreCase(setting)) {
                    yield false;
                }
                yield options.contains(name);
            }
            case DENIED -> {
                if (ALL.equalsIgnoreCase(setting)) {
                    yield false;
                }
                if (NONE.equalsIgnoreCase(setting)) {
                    yield true;
                }
                yield !options.contains(name);
            }
        };
    }

    private Policy resolvePolicy(final Mode mode, final String systemPropertyName, final String environmentVariableName,
                                        final Map<String, String> environment, final Properties systemProperties) {
        final var systemPropertyValue = systemProperties.getProperty(systemPropertyName);
        if (!Strings.isNullOrBlank(systemPropertyValue)) {
            return new Policy(mode, systemPropertyValue.trim(), "system property " + systemPropertyName);
        }
        final var environmentValue = environment.get(environmentVariableName);
        if (!Strings.isNullOrBlank(environmentValue)) {
            return new Policy(mode, environmentValue.trim(), "environment variable " + environmentVariableName);
        }
        return null;
    }

    private Set<String> parseOptionNames(final String setting) {
        return Arrays.stream(setting.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }
}
