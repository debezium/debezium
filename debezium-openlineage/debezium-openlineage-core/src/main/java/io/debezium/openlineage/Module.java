/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.Properties;

import io.debezium.util.IoUtil;

public class Module {
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/openlineage/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

}
