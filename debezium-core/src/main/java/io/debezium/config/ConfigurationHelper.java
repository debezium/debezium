/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;

class ConfigurationHelper {

    @SuppressWarnings("unchecked")
    static <T> T doGetInstance(String className, Supplier<ClassLoader> classloaderSupplier,
            Configuration configuration) {
        if (className != null) {
            ClassLoader classloader = classloaderSupplier != null ? classloaderSupplier.get()
                    : Configuration.class.getClassLoader();
            try {
                Class<? extends T> clazz = (Class<? extends T>) classloader.loadClass(className);
                return configuration == null ? clazz.newInstance()
                        : clazz.getConstructor(Configuration.class).newInstance(configuration);
            } catch (ClassNotFoundException e) {
                LoggerFactory.getLogger(Configuration.class).error("Unable to find class {}", className, e);
            } catch (InstantiationException e) {
                LoggerFactory.getLogger(Configuration.class).error("Unable to instantiate class {}", className, e);
            } catch (IllegalAccessException e) {
                LoggerFactory.getLogger(Configuration.class).error("Unable to access class {}", className, e);
            } catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                LoggerFactory.getLogger(Configuration.class).error("Call constructor(Configuration) of class {} failed",
                        className, e);
            }
        }
        return null;
    }
}
