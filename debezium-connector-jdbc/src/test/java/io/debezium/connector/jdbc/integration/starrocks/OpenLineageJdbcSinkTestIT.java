/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractOpenLineageJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;

/**
 * OpenLineage integration tests for StarRocks.
 */
@Tag("all")
@Tag("it")
@Tag("it-starrocks")
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class OpenLineageJdbcSinkTestIT extends AbstractOpenLineageJdbcSinkTest {

    public OpenLineageJdbcSinkTestIT(Sink sink) {
        super(sink);
    }
}
