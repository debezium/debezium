/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

/**
 * A wrapper over MySqlConnector
 * to allow multiple plugin versions support in CCloud
 */
public class MySqlConnectorV2 extends MySqlConnector {

    public MySqlConnectorV2() {
    }
}
