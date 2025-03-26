/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;

/**
 * A custom implementation of the {@link io.debezium.sink.naming.ColumnNamingStrategy} interface
 * that supports various naming styles and customizations such as snake_case,
 * camelCase, and prefix/suffix additions.
 *
 * @author Gustavo Lira
 */
public class CustomColumnNamingStrategy implements ColumnNamingStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomColumnNamingStrategy.class);

    // Configuration property keys
    private static final String PREFIX_PROPERTY = "column.naming.prefix";
    private static final String SUFFIX_PROPERTY = "column.naming.suffix";
    private static final String STYLE_PROPERTY = "column.naming.style";

    private String prefix = "";
    private String suffix = "";
    private NamingStyle namingStyle = NamingStyle.DEFAULT;

    /**
     * Resolves the column name by applying the configured naming style, prefix, and suffix.
     *
     * @param fieldName the original field name to transform
     * @return the transformed column name
     */
    @Override
    public String resolveColumnName(String fieldName) {
        if (fieldName == null) {
            return null;
        }

        LOGGER.debug("Resolving column name '{}' with style='{}', prefix='{}', suffix='{}'",
                fieldName, namingStyle.getValue(), prefix, suffix);

        String transformedName = NamingStyleUtils.applyNamingStyle(fieldName, namingStyle);
        String result = prefix + transformedName + suffix;

        LOGGER.debug("Column name transformed: '{}' -> '{}'", fieldName, result);
        return result;
    }

    /**
     * Configures the naming strategy using the provided properties map.
     *
     * @param props the map of configuration properties
     */
    @Override
    public void configure(Map<String, String> props) {
        prefix = props.getOrDefault(PREFIX_PROPERTY, "");
        suffix = props.getOrDefault(SUFFIX_PROPERTY, "");

        String styleValue = props.getOrDefault(STYLE_PROPERTY, "default");
        namingStyle = NamingStyle.from(styleValue);

        LOGGER.debug("Configured with prefix='{}', suffix='{}', style='{}'",
                prefix, suffix, namingStyle.getValue());
    }
}