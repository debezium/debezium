/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigValue;

/**
 * Details about an invalid {@link Configuration}.
 * 
 * @see Configuration#validateAndThrow(Field.Set)
 * @see Configuration#validateAndThrow(io.debezium.config.Field.Set, String)
 * @author Randall Hauch
 */
public class InvalidConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final Map<String, ConfigValue> invalidConfigValues;

    /**
     * Create an exception with a message and the invalid configuration values.
     * 
     * @param message the message; may not be null
     * @param configValues the configuration values, at least one of which is invalid and has errors; may not be null
     */
    public InvalidConfigurationException(String message, Iterable<ConfigValue> configValues) {
        super(message);
        Map<String, ConfigValue> invalidConfigValuesByName = new HashMap<>();
        configValues.forEach(configValue -> {
            List<String> errorMsgs = configValue.errorMessages();
            if (errorMsgs != null && !errorMsgs.isEmpty()) {
                invalidConfigValuesByName.put(configValue.name(), configValue);
            }
        });
        assert !invalidConfigValuesByName.isEmpty();
        this.invalidConfigValues = Collections.unmodifiableMap(invalidConfigValuesByName);
    }

    /**
     * Get the configuration values that have errors.
     * 
     * @return the immutable map of invalid configuration values; never null
     */
    public Map<String, ConfigValue> invalidConfigValues() {
        return invalidConfigValues;
    }

    /**
     * Call the specified function on each of the invalid configuration values.
     * 
     * @param consumer the function to be called; may not be null
     */
    public void forEach(Consumer<ConfigValue> consumer) {
        this.invalidConfigValues.values().forEach(consumer);
    }
}
