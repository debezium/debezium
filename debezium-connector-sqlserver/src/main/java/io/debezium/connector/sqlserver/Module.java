/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 *
 * @author Jiri Pechanec
 */
public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/sqlserver/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    public static String name() {
        return "sqlserver";
    }
}
