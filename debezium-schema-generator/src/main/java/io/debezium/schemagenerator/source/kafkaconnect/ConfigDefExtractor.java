/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Extracts Kafka Connect {@link ConfigDef} from component classes.
 *
 * <p>This extractor tries multiple approaches to obtain a component's configuration definition:
 * <ol>
 *   <li><b>Static CONFIG_DEF field</b> - Most common pattern in KC components</li>
 *   <li><b>config() method</b> - Fallback for components using the Configurable interface</li>
 * </ol>
 *
 * <p>If no ConfigDef can be extracted, an empty ConfigDef is returned rather than null.
 *
 * <p><b>Example KC patterns:</b>
 * <pre>{@code
 * // Pattern 1: Static CONFIG_DEF field
 * public class MyTransform implements Transformation {
 *     public static final ConfigDef CONFIG_DEF = new ConfigDef()
 *         .define("my.config", ConfigDef.Type.STRING, ...);
 * }
 *
 * // Pattern 2: config() method
 * public class MyConverter implements Converter {
 *     public ConfigDef config() {
 *         return new ConfigDef().define(...);
 *     }
 * }
 * }</pre>
 */
public class ConfigDefExtractor {

    private static final Logger LOGGER = System.getLogger(ConfigDefExtractor.class.getName());

    private static final String CONFIG_DEF_FIELD_NAME = "CONFIG_DEF";
    private static final String CONFIG_METHOD_NAME = "config";

    /**
     * Extracts ConfigDef from a Kafka Connect component class.
     *
     * <p>Tries multiple extraction approaches in order:
     * <ol>
     *   <li>Static CONFIG_DEF field</li>
     *   <li>config() method on new instance</li>
     * </ol>
     *
     * @param componentClass the component class to extract from
     * @return Optional containing ConfigDef if found, or Optional.empty() if extraction fails
     */
    public Optional<ConfigDef> extractConfigDef(Class<?> componentClass) {

        ConfigDef configDef = tryStaticField(componentClass);
        if (configDef != null) {
            return Optional.of(configDef);
        }

        configDef = tryConfigMethod(componentClass);
        if (configDef != null) {
            return Optional.of(configDef);
        }

        LOGGER.log(Level.WARNING,
                "Could not extract ConfigDef from " + componentClass.getName() +
                        ". Component may have no configuration or use non-standard pattern.");

        return Optional.empty();
    }

    /**
     * Attempts to extract ConfigDef from a static CONFIG_DEF field.
     *
     * <p>This is the most common pattern in Kafka Connect components.
     *
     * @param clazz the class to examine
     * @return ConfigDef from field, or null if not found
     */
    private ConfigDef tryStaticField(Class<?> clazz) {
        try {
            Field field = clazz.getDeclaredField(CONFIG_DEF_FIELD_NAME);
            field.setAccessible(true);
            Object value = field.get(null); // null because it's static

            if (value instanceof ConfigDef) {
                LOGGER.log(Level.DEBUG,
                        "Found ConfigDef via " + CONFIG_DEF_FIELD_NAME + " field in " + clazz.getName());
                return (ConfigDef) value;
            }
        }
        catch (NoSuchFieldException e) {
            // Expected for classes without CONFIG_DEF field
            LOGGER.log(Level.DEBUG,
                    "No " + CONFIG_DEF_FIELD_NAME + " field in " + clazz.getName());
        }
        catch (Exception e) {
            LOGGER.log(Level.DEBUG,
                    "Could not access " + CONFIG_DEF_FIELD_NAME + " in " + clazz.getName(), e);
        }

        return null;
    }

    /**
     * Attempts to extract ConfigDef by calling the config() method.
     *
     * <p>This requires instantiating the class using its no-arg constructor.
     * If instantiation fails, this approach is skipped.
     *
     * @param clazz the class to examine
     * @return ConfigDef from config() method, or null if not available
     */
    private ConfigDef tryConfigMethod(Class<?> clazz) {
        try {
            // Instantiate using no-arg constructor
            Object instance = clazz.getDeclaredConstructor().newInstance();

            // Call config() method
            Method method = clazz.getMethod(CONFIG_METHOD_NAME);
            Object result = method.invoke(instance);

            if (result instanceof ConfigDef) {
                LOGGER.log(Level.DEBUG,
                        "Got ConfigDef from " + CONFIG_METHOD_NAME + "() method in " + clazz.getName());
                return (ConfigDef) result;
            }
        }
        catch (NoSuchMethodException e) {
            // Expected for classes without config() method
            LOGGER.log(Level.DEBUG,
                    "No " + CONFIG_METHOD_NAME + "() method in " + clazz.getName());
        }
        catch (Exception e) {
            LOGGER.log(Level.DEBUG,
                    "Could not invoke " + CONFIG_METHOD_NAME + "() on " + clazz.getName(), e);
        }

        return null;
    }
}
