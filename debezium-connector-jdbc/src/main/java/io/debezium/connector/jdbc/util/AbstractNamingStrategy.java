/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.Map;

import io.debezium.DebeziumException;

/**
 * Base class for naming strategies that provides common functionality for applying prefixes, suffixes,
 * and naming styles (e.g., snake_case, camelCase, etc.) to names.
 * This class can be extended by naming strategies for columns, collections, or other entities that require
 * customizable naming conventions.
 *
 * @author Gustavo Lira
 */
public abstract class AbstractNamingStrategy {

    protected String prefix = "";
    protected String suffix = "";
    protected NamingStyle namingStyle = NamingStyle.DEFAULT;

    /**
     * Configures the naming strategy using the provided properties.
     *
     * @param properties the map of configuration properties
     * @param prefixKey the key used to retrieve the prefix property
     * @param suffixKey the key used to retrieve the suffix property
     * @param styleKey the key used to retrieve the naming style property
     */
    public void configure(Map<String, String> properties, String prefixKey, String suffixKey, String styleKey) {
        this.prefix = properties.getOrDefault(prefixKey, "");
        this.suffix = properties.getOrDefault(suffixKey, "");
        this.namingStyle = NamingStyle.from(properties.getOrDefault(styleKey, NamingStyle.DEFAULT.name()));
    }

    /**
     * Applies the configured naming style, prefix, and suffix to the given name.
     *
     * @param name the original name to be transformed
     * @return the transformed name with applied prefix, suffix, and naming style
     */
    public String applyNaming(String name) {
        if (name == null) {
            throw new DebeziumException("Name must not be null");
        }
        String transformedName = NamingStyleUtils.applyNamingStyle(name, namingStyle);
        return prefix + transformedName + suffix;
    }
}
