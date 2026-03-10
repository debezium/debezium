/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

/**
 * Utility class dealing with Java version information.
 *
 * @author Gunnar Morling
 */
public class JvmVersionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JvmVersionUtil.class);
    private static final int FEATURE_VERSION = determineFeatureVersion();

    private JvmVersionUtil() {
    }

    /**
     * Returns the feature version of the current JVM, e.g. 8 or 17.
     */
    private static int determineFeatureVersion() {
        int featureVersion;

        // Trying Runtime.version().version().get(0) at first; this will return the major version on Java 9+
        //
        // Using method handles so to be able to compile this code to Java 1.8 byte code level using the --release
        // parameter
        //
        // Using the List<Integer> version() approach, so to avoid calling the deprecated majorVersion() method
        try {
            ClassLoader classLoader = JvmVersionUtil.class.getClassLoader();

            Class<?> versionClass = classLoader.loadClass("java.lang.Runtime$Version");
            MethodHandle versionHandle = MethodHandles.lookup().findStatic(Runtime.class, "version", methodType(versionClass));
            MethodHandle versionListHandle = MethodHandles.lookup().findVirtual(versionClass, "version", methodType(List.class));

            try {
                Object version = versionHandle.invoke();
                List<Integer> versions = (List<Integer>) versionListHandle.bindTo(version).invoke();
                featureVersion = versions.get(0);
            }
            catch (Throwable e) {
                throw new DebeziumException("Couldn't determine runtime version", e);
            }
        }
        // The version() method is only available on Java 9 and later
        catch (ClassNotFoundException | NoSuchMethodError | NoSuchMethodException | IllegalAccessException nsme) {
            final String specVersion = System.getProperty("java.specification.version");
            featureVersion = Integer.parseInt(specVersion.substring(specVersion.indexOf('.') + 1));
        }

        LOGGER.debug("Determined Java version: {}", featureVersion);

        return featureVersion;
    }

    public static int getFeatureVersion() {
        return FEATURE_VERSION;
    }
}
