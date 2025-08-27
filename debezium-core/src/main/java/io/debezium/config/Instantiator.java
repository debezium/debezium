/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Instantiates given classes reflectively.
 *
 * @author Jiri Pechanec
 */
public class Instantiator {

    public static ClassLoader getClassLoader() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        if (classloader == null) {
            classloader = Configuration.class.getClassLoader();
        }

        return classloader;
    }

    /**
     * Instantiates the specified class using the no-args constructor.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstance(String className) {
        return getInstanceWithProvidedConstructorType(className, null, null);
    }

    /**
     * Instantiates the specified class using specified class loader and the no-args constructor.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstance(String className, ClassLoader classLoader) {
        return getInstanceWithProvidedConstructorType(className, null, null, classLoader);
    }

    /**
     * Instantiates the specified class either using the no-args constructor or the
     * constructor with a single parameter of type {@link Configuration}, if a
     * configuration object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstance(String className, Configuration configuration) {
        return getInstanceWithProvidedConstructorType(className, Configuration.class, configuration);
    }

    /**
     * Instantiates the specified class using provided class loader and either using the no-args
     * constructor or the constructor with a single parameter of type {@link Configuration}, if a
     * configuration object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstance(String className, Configuration configuration, ClassLoader classLoader) {
        return getInstanceWithProvidedConstructorType(className, Configuration.class, configuration, classLoader);
    }

    /**
     * Instantiates the specified class either using the no-args constructor or the
     * constructor with a single parameter of type {@link Properties}, if a
     * properties object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstanceWithProperties(String className, Properties prop) {
        return getInstanceWithProvidedConstructorType(className, Properties.class, prop);
    }

    /**
     * Instantiates the specified class using provided class loader and either using the no-args
     * constructor or the constructor with a single parameter of type {@link Properties}, if a
     * properties object is passed.
     *
     * @return The newly created instance or {@code null} if no class name was given
     */
    public static <T> T getInstanceWithProperties(String className, Properties prop, ClassLoader classLoader) {
        return getInstanceWithProvidedConstructorType(className, Properties.class, prop, classLoader);
    }

    @SuppressWarnings("unchecked")
    public static <T, C> T getInstanceWithProvidedConstructorType(String className, Class<C> constructorType, C constructorValue) {
        return getInstanceWithProvidedConstructorType(className, constructorType, constructorValue, null);
    }

    @SuppressWarnings("unchecked")
    public static <T, C> T getInstanceWithProvidedConstructorType(String className, Class<C> constructorType, C constructorValue, ClassLoader cl) {
        if (className != null) {
            ClassLoader classloader = cl == null ? Instantiator.getClassLoader() : cl;
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
