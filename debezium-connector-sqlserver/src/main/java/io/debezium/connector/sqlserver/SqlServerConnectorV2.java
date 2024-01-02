/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

/**
 * A wrapper over SqlServerConnector
 * to allow multiple plugin versions support in CCloud
 *
 */
public class SqlServerConnectorV2 extends SqlServerConnector {

    public SqlServerConnectorV2() {

    }
}
