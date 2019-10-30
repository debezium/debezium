/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.config.Configuration;

/**
 * A functional interface for obtaining the value of variables given their names.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
public interface AvailableVariables {

    /**
     * Get an {@link AvailableVariables} that returns the value of a {@link Configuration#getString(String) Configuration
     * property}.
     *
     * @param config the configuration
     * @return the available variables function that returns the configuration property value for the given property name; never
     *         null
     */
    public static AvailableVariables configVariables(Configuration config) {
        return config::getString;
    }

    /**
     * Get an {@link AvailableVariables} that returns {@link System#getProperty(String) System properties} set with
     * {@code -Dvariable=value}.
     * <p>
     * This method does not use the configuration.
     *
     * @param config the configuration
     * @return the available variables function that returns System properties; never null
     */
    public static AvailableVariables systemVariables(Configuration config) {
        return System::getProperty;
    }

    /**
     * Get an {@link AvailableVariables} that returns {@link System#getenv(String) System environment variables} set with
     * operating system environment variables (e.g., <code>${JAVA_HOME}</code>).
     * <p>
     * This method does not use the configuration.
     *
     * @param config the configuration
     * @return the available variables function that returns System properties; never null
     */
    public static AvailableVariables environmentVariables(Configuration config) {
        return System::getenv;
    }

    /**
     * Get an {@link AvailableVariables} that always returns null.
     *
     * @return the empty available variables function that always return null.
     */
    public static AvailableVariables empty() {
        return (varName) -> null;
    }

    /**
     * Obtain the value of a variable with the given name.
     *
     * @param varName the variable name; may be null
     * @return the value of the variable; may be null if the named variable is not known
     */
    String variableForName(String varName);

    /**
     * Obtain an {@link AvailableVariables} function that first checks this instance and then checks the supplied instance.
     *
     * @param next the next {@link AvailableVariables} instance to check
     * @return the new {@link AvailableVariables} function that combines both instances; never null but may be this instance if
     *         {@code next} is null or {@this}
     */
    default AvailableVariables and(AvailableVariables next) {
        if (next == null || next == this) {
            return this;
        }
        return (varName) -> {
            String result = this.variableForName(varName);
            if (result == null) {
                result = next.variableForName(varName);
            }
            return result;
        };
    }
}
