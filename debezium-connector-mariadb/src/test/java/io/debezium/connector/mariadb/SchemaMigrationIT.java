/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogSchemaMigrationIT;
import io.debezium.connector.mariadb.antlr.listener.RenameTableParserListener;

/**
 * @author Chris Cranford
 */
public class SchemaMigrationIT extends BinlogSchemaMigrationIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Class<?> getRenameTableParserListenerClass() {
        return RenameTableParserListener.class;
    }
}
