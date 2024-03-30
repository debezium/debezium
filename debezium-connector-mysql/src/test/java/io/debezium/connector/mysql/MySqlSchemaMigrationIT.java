/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogSchemaMigrationIT;
import io.debezium.connector.mysql.antlr.listener.RenameTableParserListener;

/**
 * @author Jiri Pechanec
 */
public class MySqlSchemaMigrationIT extends BinlogSchemaMigrationIT<MySqlConnector> implements MySqlCommon {
    @Override
    protected Class<?> getRenameTableParserListenerClass() {
        return RenameTableParserListener.class;
    }
}
