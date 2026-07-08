/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.data.Index;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkCloudEventTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;

/**
 * Converted CloudEvent saving tests for StarRocks.
 */
@Tag("all")
@Tag("it")
@Tag("it-starrocks")
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class JdbcSinkCloudEventIT extends AbstractJdbcSinkCloudEventTest {

    public JdbcSinkCloudEventIT(Sink sink) {
        super(sink);
    }

    @Override
    protected void assertHasPrimaryKeyColumns(String tableName, boolean caseInsensitive, String... columnNames) {
        // The StarRocks JDBC driver does not expose primary keys through JDBC metadata.
        List<String> pkColumnNames = StarRocksTestUtils.getPrimaryKeyColumnNames(dataSource(), tableName);
        if (columnNames.length == 0) {
            assertThat(pkColumnNames).isEmpty();
        }
        else if (caseInsensitive) {
            pkColumnNames = pkColumnNames.stream().map(String::toLowerCase).collect(Collectors.toList());
            assertThat(pkColumnNames.size()).isEqualTo(columnNames.length);
            for (int columnIndex = 0; columnIndex < columnNames.length; ++columnIndex) {
                assertThat(pkColumnNames).contains(columnNames[columnIndex].toLowerCase(), Index.atIndex(columnIndex));
            }
        }
        else {
            // noinspection ConfusingArgumentToVarargsMethod
            assertThat(pkColumnNames).containsExactly(columnNames);
        }
    }
}
