/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Information about this module.
 *
 */
public final class Module {

    private static final Properties INFO = loadProperties("io/debezium/kcrestextension/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * Atomically load the properties file at the given location within the designated class loader.
     *
     * @param classpathResource the path to the resource file; may not be null
     * @return the properties object; never null, but possibly empty
     * @throws IllegalStateException if the file could not be found or read
     */
    private static Properties loadProperties(String classpathResource) {
        var classLoader = Module.class.getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(classpathResource)) {
            Properties props = new Properties();
            props.load(stream);
            return props;
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to find or read the '" + classpathResource + "' file using the " +
                    classLoader + " class loader", e);
        }
    }
}
