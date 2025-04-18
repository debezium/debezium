/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.binlog.util.UniqueDatabaseProvider;

/**
 * A Java ServiceLoader contract of {@link UniqueDatabaseProvider} to supply the test suite with the class
 * reference to the MariaDB connector's specific {@link UniqueDatabase} implementation.
 *
 * @author Chris Cranford
 */
public class MariaDbUniqueDatabaseProvider implements UniqueDatabaseProvider {
    @Override
    public Class<? extends UniqueDatabase> getUniqueDatabase() {
        return MariaDbUniqueDatabase.class;
    }
}
