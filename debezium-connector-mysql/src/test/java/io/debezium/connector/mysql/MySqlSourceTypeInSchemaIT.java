/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.MapAssert.entry;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests around {@code DECIMAL} columns. Keep in sync with {@link MySqlNumericColumnIT}.
 *
 * @author Gunnar Morling
 */
public class MySqlSourceTypeInSchemaIT extends AbstractConnectorTest {

    private static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";
    private static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";
    private static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-schema-parameter.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("schemaparameterit", "source_type_as_schema_parameter_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor({ "DBZ-644", "DBZ-1222" })
    public void shouldPropagateSourceTypeAsSchemaParameter() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with("column.propagate.source.type", ".*c1,.*c2,.*c3.*,.*f.")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 1;
        int numInserts = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numInserts);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("dbz_644_source_type_mapped_as_schema_parameter_test"));
        assertThat(dmls).hasSize(1);

        SourceRecord insert = dmls.get(0);
        Field before = insert.valueSchema().field("before");

        // no type info requested as per given regexps
        Map<String, String> idSchemaParameters = before
                .schema()
                .field("id")
                .schema()
                .parameters();

        assertThat(idSchemaParameters).isNull();

        // fixed width, name but no length info
        Map<String, String> c1SchemaParameters = before
                .schema()
                .field("c1")
                .schema()
                .parameters();

        assertThat(c1SchemaParameters).includes(entry(TYPE_NAME_PARAMETER_KEY, "INT"));

        // fixed width, name but no length info
        Map<String, String> c2SchemaParameters = before
                .schema()
                .field("c2")
                .schema()
                .parameters();

        assertThat(c2SchemaParameters).includes(entry(TYPE_NAME_PARAMETER_KEY, "MEDIUMINT"));

        // variable width, name and length info
        Map<String, String> c3aSchemaParameters = before
                .schema()
                .field("c3a")
                .schema()
                .parameters();

        assertThat(c3aSchemaParameters).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMERIC"), entry(TYPE_LENGTH_PARAMETER_KEY, "5"), entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        // variable width, name and length info
        Map<String, String> c3bSchemaParameters = before
                .schema()
                .field("c3b")
                .schema()
                .parameters();

        assertThat(c3bSchemaParameters).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR"), entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        // float info
        Map<String, String> f1SchemaParameters = before
                .schema()
                .field("f1")
                .schema()
                .parameters();

        assertThat(f1SchemaParameters).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "FLOAT"), entry(TYPE_LENGTH_PARAMETER_KEY, "10"));

        Map<String, String> f2SchemaParameters = before
                .schema()
                .field("f2")
                .schema()
                .parameters();

        assertThat(f2SchemaParameters).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "FLOAT"), entry(TYPE_LENGTH_PARAMETER_KEY, "8"), entry(TYPE_SCALE_PARAMETER_KEY, "4"));
    }
}
