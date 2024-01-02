/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

/**
 * A wrapper over PostgresConnector
 * to allow multiple plugin versions support in CCloud
 */
public class PostgresConnectorV2 extends PostgresConnector {

    public PostgresConnectorV2() {
    }
}
