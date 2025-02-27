/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.oracle;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSaveConvertedCloudEventTest;
import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Converted CloudEvent saving tests for Oracle
 *
 * @author Roman Kudryashov
 */
@Tag("all")
@Tag("it")
@Tag("it-oracle")
@ExtendWith(OracleSinkDatabaseContextProvider.class)
public class JdbcSinkSaveConvertedCloudEventIT extends AbstractJdbcSinkSaveConvertedCloudEventTest {

    public JdbcSinkSaveConvertedCloudEventIT(Sink sink) {
        super(sink);
    }

}
