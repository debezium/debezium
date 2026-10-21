/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.DefaultRecordWriter;
import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnector;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorTask;
import io.debezium.connector.jdbc.JdbcSinkTaskTestContext;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkSchemaEvolutionTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordBuilder;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Schema Evolution tests for PostgreSQL
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkSchemaEvolutionIT extends AbstractJdbcSinkSchemaEvolutionTest {

    public JdbcSinkSchemaEvolutionIT(Sink sink) {
        super(sink);
    }

    @Override
    protected String getDatabaseSchemaName() {
        return "public";
    }

    @Test
    @FixFor("debezium/dbz#2302")
    public void testConcurrentAlterRecoversWhenColumnAlreadyExists() throws Exception {
        final Race race = startLosingAlter(AlterLoss.RECOVER, "order_status");
        try {
            final JdbcSinkConnectorTask competingTask = awaitCompetingPut(race);
            assertThat(competingTask.getLastProcessingException()).as("competing task failure").isNull();
            competingTask.preCommit(Map.of(new TopicPartition(race.topicName, 0), new OffsetAndMetadata(1)));
            assertThat(race.logs.countOccurrences("rechecking schema before failing")).isEqualTo(1);

            final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), race.destinationTableName);
            tableAssert.hasNumberOfRows(2);
            getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
            getSink().assertColumnType(tableAssert, "order_status", ValueType.TEXT, null, "open");
        }
        finally {
            stopRace(race);
        }
    }

    @Test
    @FixFor("debezium/dbz#2302")
    public void testConcurrentAlterFailsOnceWhenColumnRemainsMissing() throws Exception {
        final Race race = startLosingAlter(AlterLoss.MISSING, "order_status");
        try {
            final JdbcSinkConnectorTask competingTask = awaitCompetingPut(race);
            assertThat(competingTask.getLastProcessingException()).as("competing task failure").isNotNull();
            assertThat(race.logs.countOccurrences("rechecking schema before failing")).isEqualTo(1);
            assertThat(attemptCount(race.destinationTableName)).isEqualTo(2);

            final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), race.destinationTableName);
            tableAssert.hasNumberOfRows(1);
            getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        }
        finally {
            stopRace(race);
        }
    }

    @Test
    @FixFor("debezium/dbz#2302")
    public void testConcurrentAlterRecoversWhenMultipleColumnsAlreadyExist() throws Exception {
        final Race race = startLosingAlter(AlterLoss.RECOVER, "order_status", "priority");
        try {
            final JdbcSinkConnectorTask competingTask = awaitCompetingPut(race);
            assertThat(competingTask.getLastProcessingException()).as("competing task failure").isNull();
            competingTask.preCommit(Map.of(new TopicPartition(race.topicName, 0), new OffsetAndMetadata(1)));
            assertThat(race.logs.countOccurrences("rechecking schema before failing")).isEqualTo(1);

            final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), race.destinationTableName);
            tableAssert.hasNumberOfRows(2);
            getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
            getSink().assertColumnType(tableAssert, "order_status", ValueType.TEXT, null, "open");
            getSink().assertColumnType(tableAssert, "priority", ValueType.TEXT, null, "high");
        }
        finally {
            stopRace(race);
        }
    }

    private JdbcSinkConnectorTask startCompetingTask(Map<String, String> properties) throws Exception {
        final JdbcSinkConnector connector = new JdbcSinkConnector();
        connector.start(properties);
        final Map<String, String> taskConfig = connector.taskConfigs(2).get(1);
        final JdbcSinkConnectorTask task = new JdbcSinkConnectorTask();
        task.initialize(new JdbcSinkTaskTestContext(taskConfig));
        task.start(taskConfig);
        return task;
    }

    private Race startLosingAlter(AlterLoss loss, String... columns) throws Exception {
        final Map<String, String> properties = evolutionProperties();
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final JdbcKafkaSinkRecord firstRecord = record(topicName, (byte) 1, 1, properties, false);
        final String destinationTableName = destinationTableName(firstRecord);

        // The first task creates the table without the new columns. The trigger then makes the second task lose its ALTER.
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();
        consume(firstRecord);
        installLosingAlterTrigger(destinationTableName, loss, columns);

        final LogInterceptor logs = new LogInterceptor(DefaultRecordWriter.class);
        final JdbcSinkConnectorTask competingTask = startCompetingTask(properties);
        final SinkRecord competingRecord = record(topicName, (byte) 2, 2, properties, true, columns).getOriginalKafkaRecord();
        competingTask.open(List.of(new TopicPartition(topicName, 0)));
        final Thread competingPut = new Thread(() -> competingTask.put(List.of(competingRecord)), "dbz2302-competing-put");
        competingPut.start();
        awaitFirstAlterFailure(destinationTableName);
        return new Race(topicName, destinationTableName, logs, competingTask, competingPut);
    }

    private JdbcSinkConnectorTask awaitCompetingPut(Race race) {
        Awaitility.await("competing task still running")
                .atMost(Duration.ofSeconds(10))
                .until(() -> !race.competingPut.isAlive());
        return race.competingTask;
    }

    private void stopRace(Race race) throws Exception {
        try {
            race.competingTask.stop();
        }
        finally {
            getSink().execute("DROP EVENT TRIGGER IF EXISTS dbz2302_lose_alter");
            getSink().execute("DROP FUNCTION IF EXISTS dbz2302_lose_alter()");
            getSink().execute("DROP TABLE IF EXISTS dbz2302_alter_attempts");
        }
    }

    private Map<String, String> evolutionProperties() {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        return properties;
    }

    private void installLosingAlterTrigger(String tableName, AlterLoss loss, String... columns) throws Exception {
        final String qualifiedTableName = tableName.contains(".") ? tableName : "public." + tableName;
        final String unqualifiedTableName = qualifiedTableName.substring(qualifiedTableName.indexOf('.') + 1);
        getSink().execute("CREATE EXTENSION IF NOT EXISTS dblink");
        getSink().execute("CREATE TABLE IF NOT EXISTS dbz2302_alter_attempts (table_name text PRIMARY KEY, attempts int NOT NULL)");
        final String recordAttempt = "INSERT INTO dbz2302_alter_attempts (table_name, attempts) VALUES (''"
                + qualifiedTableName + "'', 1) ON CONFLICT (table_name) DO UPDATE SET attempts = dbz2302_alter_attempts.attempts + 1";
        final String dblink = "dbname=' || current_database() || ' user=" + getSink().getUsername()
                + " password=" + getSink().getPassword();
        final StringBuilder onFirstAttempt = new StringBuilder();
        if (loss == AlterLoss.RECOVER) {
            final StringBuilder addColumns = new StringBuilder("ALTER TABLE ").append(qualifiedTableName);
            for (int i = 0; i < columns.length; i++) {
                addColumns.append(i == 0 ? " ADD COLUMN " : ", ADD COLUMN ");
                addColumns.append(columns[i]).append(" text");
            }
            onFirstAttempt.append("PERFORM dblink_exec('").append(dblink).append("', '")
                    .append(addColumns).append("'); ");
        }
        onFirstAttempt.append("RAISE EXCEPTION 'column \"%\" of relation \"%\" already exists', '")
                .append(columns[0]).append("', '").append(unqualifiedTableName)
                .append("' USING ERRCODE = '42701'; ");
        final String onSecondAttempt = loss == AlterLoss.MISSING
                ? "IF attempt_count = 2 THEN RAISE EXCEPTION 'column \"%\" of relation \"%\" already exists', '"
                        + columns[0] + "', '" + unqualifiedTableName + "' USING ERRCODE = '42701'; END IF; "
                : "";
        getSink().execute("CREATE OR REPLACE FUNCTION dbz2302_lose_alter() RETURNS event_trigger LANGUAGE plpgsql AS $$ "
                + "DECLARE attempt_count int; "
                + "BEGIN "
                + "IF TG_TAG = 'ALTER TABLE' AND position('" + unqualifiedTableName + "' in current_query()) > 0 THEN "
                + "PERFORM dblink_exec('" + dblink + "', '" + recordAttempt + "'); "
                + "SELECT attempts INTO attempt_count FROM dbz2302_alter_attempts WHERE table_name = '" + qualifiedTableName + "'; "
                + "IF attempt_count = 1 THEN "
                + onFirstAttempt
                + "END IF; "
                + onSecondAttempt
                + "END IF; "
                + "END $$");
        getSink().execute("DROP EVENT TRIGGER IF EXISTS dbz2302_lose_alter");
        getSink().execute("CREATE EVENT TRIGGER dbz2302_lose_alter ON ddl_command_start EXECUTE FUNCTION dbz2302_lose_alter()");
    }

    private void awaitFirstAlterFailure(String tableName) {
        Awaitility.await("competing ALTER did not fail")
                .atMost(Duration.ofSeconds(10))
                .until(() -> attemptCount(tableName) >= 1);
    }

    private int attemptCount(String tableName) throws Exception {
        final String qualifiedTableName = tableName.contains(".") ? tableName : "public." + tableName;
        try (Statement statement = getSink().getConnection().createStatement();
                ResultSet attempts = statement.executeQuery(
                        "SELECT attempts FROM dbz2302_alter_attempts WHERE table_name = '" + qualifiedTableName + "'")) {
            if (attempts.next()) {
                return attempts.getInt(1);
            }
            return 0;
        }
    }

    private enum AlterLoss {
        RECOVER,
        MISSING
    }

    private JdbcKafkaSinkRecord record(String topicName, byte id, int offset, Map<String, String> properties,
                                       boolean includeEvolvedFields, String... columns) {
        final DebeziumSinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final SchemaBuilder recordSchema = SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA);
        if (includeEvolvedFields) {
            for (String column : columns) {
                recordSchema.field(column, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }
        final SinkRecordBuilder.SinkRecordTypeBuilder builder = factory.createBuilder(getConfig(properties))
                .topic(topicName)
                .offset(offset)
                .keySchema(factory.basicKeySchema())
                .sourceSchema(factory.basicSourceSchema())
                .recordSchema(recordSchema.build())
                .key("id", id)
                .after("id", id);
        if (includeEvolvedFields) {
            for (String column : columns) {
                builder.after(column, column.equals("priority") ? "high" : "open");
            }
        }
        return builder.build();
    }

    private record Race(String topicName, String destinationTableName, LogInterceptor logs,
            JdbcSinkConnectorTask competingTask, Thread competingPut) {
    }
}
