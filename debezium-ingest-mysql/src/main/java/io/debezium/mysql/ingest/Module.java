/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 * 
 * @author Randall Hauch
 */
public class Module {
    
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/mysql/build.properties");

    public static String version() {
        return INFO.getProperty("version");
    }
}
