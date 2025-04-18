/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this MariaDb connector module.
 *
 * @author Chris Cranford
 */
public class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/mariadb/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * Get the connector's symbolic name.
     *
     * @return the connector plug-in's symbolic name, never {@code null}.
     */
    public static String name() {
        return "mariadb";
    }

    /**
     * Get the connector context name used for logging and JMX.
     *
     * @return the context name, never {@code null}
     */
    public static String contextName() {
        return "MariaDB";
    }
}
