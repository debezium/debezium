/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;

/**
 * Utility class for extracting component metadata using reflection.
 */
public final class ComponentMetadataUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentMetadataUtils.class);

    private ComponentMetadataUtils() {
        // Utility class
    }

    /**
     * Extracts all static final Field constants from one or more classes using reflection.
     * This ensures that new fields are automatically included without manual updates.
     * Uses setAccessible(true) to access private fields, so Field constants don't need to be public.
     *
     * @deprecated Use {@link io.debezium.config.ConfigDescriptor} interface instead for explicit field declaration.
     *             Components should implement ConfigDescriptor and provide their fields via getConfigFields() method.
     * @param classes one or more classes to extract Field constants from
     * @return Field.Set containing all discovered Field constants
     */
    @Deprecated
    public static Field.Set extractFieldConstants(Class<?>... classes) {
        List<Field> fields = new ArrayList<>();

        for (Class<?> clazz : classes) {
            for (java.lang.reflect.Field declaredField : clazz.getDeclaredFields()) {
                int modifiers = declaredField.getModifiers();

                if (Modifier.isStatic(modifiers)
                        && Modifier.isFinal(modifiers)
                        && declaredField.getType().equals(Field.class)) {
                    try {
                        declaredField.setAccessible(true);
                        Field field = (Field) declaredField.get(null);
                        fields.add(field);
                    }
                    catch (IllegalAccessException e) {
                        // Skip if access fails
                        LOGGER.debug("Unable to access field {}", declaredField.getName(), e);
                    }
                }
            }
        }

        return Field.setOf(fields.toArray(new Field[0]));
    }
}
