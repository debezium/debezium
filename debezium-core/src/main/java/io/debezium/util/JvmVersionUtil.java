/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

/**
 * Utility class dealing with Java version information.
 *
 * @author Gunnar Morling
 */
public class JvmVersionUtil {

    private JvmVersionUtil() {
    }

    /**
     * Returns the feature version of the current JVM, e.g. 8 or 17.
     */
    public static int getFeatureVersion() {
        try {
            return Runtime.version().feature();
        }
        // The version() is only available on Java 9 and later
        catch (NoSuchMethodError nsme) {
            final String specVersion = System.getProperty("java.specification.version");
            return Integer.parseInt(specVersion.substring(specVersion.indexOf('.') + 1));
        }
    }
}
