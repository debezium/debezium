/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Instantiates given classes reflectively.
 *
 * @author Jiri Pechanec
 */
class Instantiator {

    /**
     * Instantiates the specified class either using the no-args constructor or the
     * constructor with a single parameter of type {@link Configuration}, if a
     * configuration object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    @SuppressWarnings("unchecked")
    static <T> T getInstance(String className, Supplier<ClassLoader> classloaderSupplier,
                             Configuration configuration) {
        if (className != null) {
            ClassLoader classloader = classloaderSupplier != null ? classloaderSupplier.get()
                    : Configuration.class.getClassLoader();
            try {
                Class<? extends T> clazz = (Class<? extends T>) classloader.loadClass(className);
                return configuration == null ? clazz.newInstance()
                        : clazz.getConstructor(Configuration.class).newInstance(configuration);
            }
            catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unable to find class" + className, e);
            }
            catch (InstantiationException e) {
                throw new IllegalArgumentException("Unable to instantiate class " + className, e);
            }
            catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to access class " + className, e);
            }
            catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new IllegalArgumentException("Call to constructor of class " + className + " failed", e);
            }
        }

        return null;
    }
}
