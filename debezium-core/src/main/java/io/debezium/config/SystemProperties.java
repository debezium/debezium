/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Configuration} often can become a System Property, this helper allows to set them in case there isn't a
 * previously effective value and keeps record of the changes for a later reset.
 *
 * @author Renato Mefi
 */
public class SystemProperties {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, String> originalSystemProperties = new HashMap<>();
    private final Configuration config;

    public SystemProperties(Configuration config) {
        this.config = config;
    }

    public void setSystemProperty(String property, Field field, boolean showValueInError) {
        String value = config.getString(field);
        if (value == null)
            return;

        value = value.trim();
        String valueOrMask = showValueInError ? value : "*********";

        String existingValue = System.getProperty(property);
        if (existingValue == null) {
            // There was no existing property ...
            String existing = System.setProperty(property, value);
            originalSystemProperties.put(property, existing); // the existing value may be null
            logger.info(String.format("Setting System property '%s' with '%s'.", property, valueOrMask));
            return;
        }

        existingValue = existingValue.trim();
        if (!existingValue.equalsIgnoreCase(value)) {
            // There was an existing property, and the value is different ...
            String msg = String.format("System or JVM property '%s' is already defined, but the configuration property '%s' defines a different value", property, field.name());
            if (showValueInError) {
                msg = String.format("System or JVM property '%s' is already defined as %s, but the configuration property '%s' defines a different value '%s'", property, existingValue, field.name(), value);
            }
            throw new ConnectException(msg);
        }

        logger.debug(String.format("Skipping setting System property '%s' since current value '%s' was already set ", property, valueOrMask));
    }

    public void resetToOriginalSystemProperties() {
        originalSystemProperties.forEach((name, value) -> {
            if (value != null) {
                System.setProperty(name, value);
            } else {
                System.clearProperty(name);
            }
        });
    }

    public void clearOriginalSystemProperties() {
        originalSystemProperties.clear();
    }
}
