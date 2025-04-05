/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSaveTransformedFieldNameTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkSaveTransformedFieldNameIT extends AbstractJdbcSinkSaveTransformedFieldNameTest {

    public JdbcSinkSaveTransformedFieldNameIT(Sink sink) {
        super(sink);
    }
}
