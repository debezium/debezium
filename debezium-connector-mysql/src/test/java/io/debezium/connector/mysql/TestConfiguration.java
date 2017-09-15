/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

public class TestConfiguration {
    public static boolean isGTIDEnabled() {
        return "true".equals(System.getProperty("test.mysql.gtid"));
    }
}
