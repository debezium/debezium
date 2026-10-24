/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogIncrementalSnapshotIT;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public class IncrementalSnapshotIT extends BinlogIncrementalSnapshotIT<MariaDbConnector> implements MariaDbCommon {
    private static final String TABLE = "year2";

    @Override
    protected Class<?> getFieldReader() {
        return MariaDbFieldReader.class;
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2773")
    public void shouldKeepYear2ZeroConsistentInIncrementalSnapshot(boolean adjuster) throws Exception {
        try (JdbcConnection connection = databaseConnection()) {
            connection.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, y YEAR(2) DEFAULT '0000')");
        }

        startConnector(config -> config
                .with(BinlogConnectorConfig.ENABLE_TIME_ADJUSTER, adjuster));

        try (JdbcConnection connection = databaseConnection()) {
            connection.execute("INSERT INTO " + TABLE + " VALUES (1, '0000'), (2, 0), (3, 2000), (4, '18'), (5, '78')");
        }

        final Map<Integer, Integer> streamedYears = consumeMixedWithIncrementalSnapshot(5,
                x -> true,
                key -> key.getInt32("id"),
                record -> year(record),
                DATABASE.topicForTable(TABLE),
                null);
        assertThat(streamedYears).containsExactlyInAnyOrderEntriesOf(expectedYears());

        sendAdHocSnapshotSignal(DATABASE.qualifiedTableName(TABLE));

        final Map<Integer, Integer> incrementalYears = consumeMixedWithIncrementalSnapshot(5,
                x -> true,
                key -> key.getInt32("id"),
                record -> year(record),
                DATABASE.topicForTable(TABLE),
                null);

        assertThat(incrementalYears).containsExactlyInAnyOrderEntriesOf(expectedYears());
        assertThat(incrementalYears).isEqualTo(streamedYears);
    }

    private static int year(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after").getInt32("y");
    }

    private static Map<Integer, Integer> expectedYears() {
        return Map.of(1, 1900, 2, 2000, 3, 2000, 4, 2018, 5, 1978);
    }
}
