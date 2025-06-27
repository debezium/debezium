/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

/**
 * Module information for the CockroachDB connector.
 *
 * @author Virag Tripathi
 */
public class Module {

    private static final String VERSION = "3.2.0-SNAPSHOT";

    public static String version() {
        return VERSION;
    }
}
