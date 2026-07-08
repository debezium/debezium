/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipColumnTypePropagation;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipExtractNewRecordState;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourcePipelineInvocationContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * End-to-end pipeline tests that write to StarRocks.
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-starrocks")
@SkipExtractNewRecordState
@SkipColumnTypePropagation
@ExtendWith(SourcePipelineInvocationContextProvider.class)
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToStarRocksIT extends AbstractJdbcSinkIT {

    @TestTemplate
    @ForSource(value = SourceType.MYSQL, reason = "Verifies a MySQL source can write to a StarRocks sink")
    public void testMySqlSourceToStarRocksSink(Source source, Sink sink) throws Exception {
        final String tableName = source.randomTableName();
        final String createSql = String.format("CREATE TABLE %s (id int primary key, data varchar(50))", tableName);
        final String insertSql = String.format("INSERT INTO %s VALUES (1, 'test')", tableName);

        registerSourceConnector(source, tableName, createSql, insertSql);

        final Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSink(source, sinkProperties, tableName);

        final SinkRecord record = consumeSinkRecord();
        final String destinationTable = record.topic().replace(".", "_");
        sink.assertColumn(destinationTable, "id", "INT");
        sink.assertColumn(destinationTable, "data", "VARCHAR");
        sink.assertRows(destinationTable, rs -> {
            assertThat(rs.getInt("id")).isEqualTo(1);
            assertThat(rs.getString("data")).isEqualTo("test");
            return null;
        });
    }

    private void registerSourceConnector(Source source, String tableName, String createSql, String insertSql) throws Exception {
        if (source.getOptions().useSnapshot()) {
            source.execute(createSql);
            source.streamTable(tableName);
            source.execute(insertSql);
            source.registerSourceConnector(getSourceConnectorConfig(source, tableName));
        }
        else {
            source.execute(createSql);
            source.streamTable(tableName);
            source.registerSourceConnector(getSourceConnectorConfig(source, tableName));
            source.execute(insertSql);
        }
    }

    private Properties getDefaultSinkConfig(Sink sink) {
        final Properties sinkProperties = new Properties();
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_URL, sink.getJdbcUrl());
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_USER, sink.getUsername());
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, sink.getPassword());
        sinkProperties.put(JdbcSinkConnectorConfig.USE_TIME_ZONE, TestHelper.getSinkTimeZone());
        return sinkProperties;
    }
}
