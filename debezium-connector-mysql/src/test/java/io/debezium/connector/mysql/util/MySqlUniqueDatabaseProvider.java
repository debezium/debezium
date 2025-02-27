/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.util;

import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.binlog.util.UniqueDatabaseProvider;
import io.debezium.connector.mysql.MySqlUniqueDatabase;

/**
 * @author Chris Cranford
 */
public class MySqlUniqueDatabaseProvider implements UniqueDatabaseProvider {
    @Override
    public Class<? extends UniqueDatabase> getUniqueDatabase() {
        return MySqlUniqueDatabase.class;
    }
}
