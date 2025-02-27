/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.db2;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkPrimaryKeyModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Db2SinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Primary Key Mode tests for DB2.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-db2")
@ExtendWith(Db2SinkDatabaseContextProvider.class)
public class JdbcSinkPrimaryKeyModeIT extends AbstractJdbcSinkPrimaryKeyModeTest {

    public JdbcSinkPrimaryKeyModeIT(Sink sink) {
        super(sink);
    }

}
