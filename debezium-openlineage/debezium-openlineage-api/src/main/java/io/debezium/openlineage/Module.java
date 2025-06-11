/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Module {
    private static final Properties INFO = loadProperties(Module.class, "io/debezium/openlineage/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    public static Properties loadProperties(Class<Module> clazz, String classpathResource) {
        // This is idempotent, so we don't need to lock ...
        ClassLoader classLoader = clazz.getClassLoader();
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