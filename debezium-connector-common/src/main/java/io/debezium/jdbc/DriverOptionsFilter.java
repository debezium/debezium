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

import io.debezium.config.Configuration;
import io.debezium.util.Strings;

/**
 * Restricts the driver pass-through options ({@code driver.*}) that are forwarded to the JDBC driver.
 * <p>
 * The operator sets the policy per connector with the system property {@code <connector>.driver.options.allowed} or the
 * environment variable {@code <CONNECTOR>_DRIVER_OPTIONS_ALLOWED}; the system property takes precedence. The value is
 * {@code ALL} (the default, no restriction), {@code NONE} (no driver option is forwarded), or a comma-separated list of
 * the driver option names that may be forwarded, for example {@code sslMode,connectTimeout}.
 */
public class DriverOptionsFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverOptionsFilter.class);

    private static final String ALL = "ALL";
    private static final String NONE = "NONE";
    private static final String SYSTEM_PROPERTY_SUFFIX = ".driver.options.allowed";
    private static final String ENVIRONMENT_VARIABLE_SUFFIX = "_DRIVER_OPTIONS_ALLOWED";

    private final String connectorName;
    private final String setting;
    private final Set<String> allowed;

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
            this.setting = ALL;
            this.allowed = Set.of();
            return;
        }

        final var systemPropertyName = connectorName + SYSTEM_PROPERTY_SUFFIX;
        final var environmentVariableName = connectorName.toUpperCase(Locale.ROOT) + ENVIRONMENT_VARIABLE_SUFFIX;
        final var systemPropertyValue = systemProperties.getProperty(systemPropertyName);
        final var environmentValue = environment.get(environmentVariableName);

        final String source;
        if (!Strings.isNullOrBlank(systemPropertyValue)) {
            this.setting = systemPropertyValue.trim();
            source = "system property " + systemPropertyName;
        }
        else if (!Strings.isNullOrBlank(environmentValue)) {
            this.setting = environmentValue.trim();
            source = "environment variable " + environmentVariableName;
        }
        else {
            this.setting = ALL;
            source = "default";
        }

        LOGGER.info("Driver pass-through options policy for connector '{}': {} (from {})", connectorName, this.setting, source);

        if (ALL.equalsIgnoreCase(this.setting)) {
            this.allowed = Set.of();
        }
        else if (NONE.equalsIgnoreCase(this.setting)) {
            this.allowed = Set.of();
        }
        else {
            this.allowed = parseOptionNames(this.setting);
        }
    }

    /**
     * Returns the driver options that the policy of this connector allows.
     *
     * @param driverOptions the driver options with the {@code driver.} prefix removed
     * @return the driver options allowed by the policy; never null
     */
    public Configuration filter(final Configuration driverOptions) {
        if (connectorName == null || ALL.equalsIgnoreCase(setting)) {
            return driverOptions;
        }

        final Set<String> filteredOut = driverOptions.keys().stream()
                .filter(name -> !allowed.contains(name))
                .collect(Collectors.toCollection(TreeSet::new));
        if (!filteredOut.isEmpty()) {
            LOGGER.warn("Driver pass-through options filtered out for connector '{}' by the policy: {}", connectorName, filteredOut);
        }
        return driverOptions.filter(allowed::contains);
    }

    private Set<String> parseOptionNames(final String setting) {
        return Arrays.stream(setting.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }
}
