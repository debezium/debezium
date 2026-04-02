/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import static java.lang.invoke.MethodHandles.privateLookupIn;
import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDefExtractor.class);

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

        LOGGER.warn("Could not extract ConfigDef from {}. Component may have no configuration or use non-standard pattern.",
                componentClass.getName());

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
            MethodHandle getter = privateLookupIn(clazz, MethodHandles.lookup())
                    .findStaticGetter(clazz, CONFIG_DEF_FIELD_NAME, ConfigDef.class);
            ConfigDef value = (ConfigDef) getter.invoke();

            if (value != null) {
                LOGGER.debug("Found ConfigDef via {} field in {}", CONFIG_DEF_FIELD_NAME, clazz.getName());
                return value;
            }
        }
        catch (NoSuchFieldException e) {
            // Expected for classes without CONFIG_DEF field
            LOGGER.debug("No {} field in {}", CONFIG_DEF_FIELD_NAME, clazz.getName());
        }
        catch (Throwable e) {
            LOGGER.debug("Could not access {} in {}", CONFIG_DEF_FIELD_NAME, clazz.getName(), e);
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
            Object instance = clazz.getDeclaredConstructor().newInstance();

            MethodHandle handle = MethodHandles.lookup().findVirtual(clazz, CONFIG_METHOD_NAME, methodType(ConfigDef.class));
            Object result = handle.invoke(instance);

            if (result instanceof ConfigDef) {
                LOGGER.debug("Got ConfigDef from {}() method in {}", CONFIG_METHOD_NAME, clazz.getName());
                return (ConfigDef) result;
            }
        }
        catch (NoSuchMethodException e) {
            // Expected for classes without config() method
            LOGGER.debug("No {}() method in {}", CONFIG_METHOD_NAME, clazz.getName());
        }
        catch (Throwable e) {
            LOGGER.debug("Could not invoke {}() on {}", CONFIG_METHOD_NAME, clazz.getName(), e);
        }

        return null;
    }
}
