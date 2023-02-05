/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Instantiates given classes reflectively.
 *
 * @author Jiri Pechanec
 */
public class Instantiator {
    public static ClassLoader getClassLoader(Supplier<ClassLoader> classloaderSupplier) {

        if (classloaderSupplier != null) {
            return classloaderSupplier.get();
        }

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        if (classloader == null) {
            classloader = Configuration.class.getClassLoader();
        }

        return classloader;
    }

    public static ClassLoader getClassLoader() {
        return Instantiator.getClassLoader(null);
    }

    /**
     * Instantiates the specified class either using the no-args constructor or the
     * constructor with a single parameter of type {@link Configuration}, if a
     * configuration object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstance(String className, Supplier<ClassLoader> classloaderSupplier,
                                    Configuration configuration) {
        return getInstanceWithProvidedConstructorType(className, classloaderSupplier, Configuration.class, configuration);
    }

    /**
     * Instantiates the specified class either using the no-args constructor or the
     * constructor with a single parameter of type {@link Properties}, if a
     * properties object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstanceWithProperties(String className, Supplier<ClassLoader> classloaderSupplier,
                                                  Properties prop) {
        return getInstanceWithProvidedConstructorType(className, classloaderSupplier, Properties.class, prop);
    }

    @SuppressWarnings("unchecked")
    public static <T, C> T getInstanceWithProvidedConstructorType(String className, Supplier<ClassLoader> classloaderSupplier, Class<C> constructorType,
                                                                  C constructorValue) {
        if (className != null) {
            ClassLoader classloader = Instantiator.getClassLoader(classloaderSupplier);
            try {
                Class<? extends T> clazz = (Class<? extends T>) classloader.loadClass(className);
                return constructorValue == null ? clazz.getDeclaredConstructor().newInstance()
                        : clazz.getConstructor(constructorType).newInstance(constructorValue);
            }
            catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unable to find class " + className, e);
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
