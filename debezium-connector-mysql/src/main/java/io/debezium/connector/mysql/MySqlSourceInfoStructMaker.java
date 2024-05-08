/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogSourceInfoStructMaker;

public class MySqlSourceInfoStructMaker extends BinlogSourceInfoStructMaker<SourceInfo> {
    @Override
    protected String getConnectorName() {
        return Module.name();
    }
}
