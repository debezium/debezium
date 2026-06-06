/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema;

import io.debezium.config.Field;

/**
 * Default field filter that excludes internal properties.
 */
public class DefaultFieldFilter implements Schema.FieldFilter {

    public static final String INTERNAL_PROPERTY_SUFFIX = "internal.";

    @Override
    public boolean include(Field field) {
        String name = field.name();

        return !name.startsWith(INTERNAL_PROPERTY_SUFFIX);
    }
}
