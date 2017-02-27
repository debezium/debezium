/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 * 
 * @author Horia Chiorean
 */
public final class Module {
    
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/postgresql/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }
}
