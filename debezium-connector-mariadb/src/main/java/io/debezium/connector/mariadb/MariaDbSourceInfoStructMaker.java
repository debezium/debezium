/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.binlog.BinlogSourceInfoStructMaker;

/**
 * The {@code source} {@link Struct} information maker for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbSourceInfoStructMaker extends BinlogSourceInfoStructMaker<SourceInfo> {
    @Override
    protected String getConnectorName() {
        return Module.name();
    }
}
