/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.naming.CustomCollectionNamingStrategy;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Integration tests for collection name transformation in PostgreSQL.
 * <p>
 * These tests verify that the {@link CustomCollectionNamingStrategy} works correctly
 * with PostgreSQL, including transformations like snake_case and adding prefix/suffix.
 *
 * @author Gustavo Lira
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkSaveTransformedCollectionNameIT extends AbstractJdbcSinkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkSaveTransformedCollectionNameIT.class);

    public JdbcSinkSaveTransformedCollectionNameIT(Sink sink) {
        super(sink);
    }

    /**
     * Tests that camelCase table names are correctly transformed to snake_case in PostgreSQL.
     * <p>
     * This test:
     * 1. Configures a connector using snake_case naming strategy
     * 2. Creates a record with a CamelCase topic/table name
     * 3. Verifies the table is created with snake_case name
     * 4. Confirms the record data is correctly stored
     *
     * @throws SQLException if database operations fail
     */
    @Test
    @FixFor("DBZ-8765")
    public void testSnakeCaseTransformation() throws SQLException {
        // Setup connector with snake_case naming style
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, CustomCollectionNamingStrategy.class.getName());
        properties.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "true");
        properties.put("collection.naming.style", "snake_case");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        // Create camelCase table name with timestamp to ensure uniqueness
        String mixedCaseTableName = "UserRegistrationRecord" + System.currentTimeMillis();
        LOGGER.info("Testing snake_case transformation with table name: {}", mixedCaseTableName);

        // Generate and consume record
        KafkaDebeziumSinkRecord record = createRecordWithStructKey(mixedCaseTableName);
        String recordId = ((Struct) record.getOriginalKafkaRecord().key()).getString("id");
        consume(record);

        // Verify table creation and data
        try (Connection conn = getSink().getConnection()) {
            List<String> allTables = getAllTables(conn);
            LOGGER.debug("Found tables in database: {}", allTables);

            // Find table matching the expected snake_case pattern
            String matchingTable = findMatchingTable(allTables, "user", "registration", "record");
            assertThat(matchingTable)
                    .as("Table with snake_case transformation should exist")
                    .isNotNull();

            // Verify record exists in table with correct data
            assertRecordExists(conn, matchingTable, recordId, "Test " + mixedCaseTableName, 42);
        }
    }

    /**
     * Tests that table names are correctly transformed with prefix and suffix.
     * <p>
     * This test:
     * 1. Configures a connector with prefix and suffix settings
     * 2. Creates a record with a standard table name
     * 3. Verifies the table is created with the prefix and suffix applied
     * 4. Confirms the record data is correctly stored
     *
     * @throws SQLException if database operations fail
     */
    @Test
    @FixFor("DBZ-8765")
    public void testPrefixSuffixTransformation() throws SQLException {
        // Setup connector with prefix and suffix
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, CustomCollectionNamingStrategy.class.getName());
        properties.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "true");
        properties.put("collection.naming.prefix", "dbz_");
        properties.put("collection.naming.suffix", "_table");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        // Create table name with timestamp to ensure uniqueness
        String tableName = "customer" + System.currentTimeMillis();
        LOGGER.info("Testing prefix/suffix transformation with table name: {}", tableName);

        // Generate and consume record
        KafkaDebeziumSinkRecord record = createRecordWithStructKey(tableName);
        String recordId = ((Struct) record.getOriginalKafkaRecord().key()).getString("id");
        consume(record);

        // Verify table creation and data
        try (Connection conn = getSink().getConnection()) {
            List<String> allTables = getAllTables(conn);
            LOGGER.debug("Found tables in database: {}", allTables);

            // Find table matching the expected prefix_name_suffix pattern
            String matchingTable = findMatchingTable(allTables, "dbz_", tableName.toLowerCase(), "_table");
            assertThat(matchingTable)
                    .as("Table with prefix and suffix should exist")
                    .isNotNull();

            // Verify record exists in table with correct data
            assertRecordExists(conn, matchingTable, recordId, "Test " + tableName, 42);
        }
    }

    /**
     * Creates a record with a structured key containing an ID field.
     * <p>
     * Using a structured key instead of a primitive allows us to avoid
     * configuring primary.key.fields explicitly.
     *
     * @param tableName the table/topic name for the record
     * @return a sink record wrapped in a KafkaDebeziumSinkRecord
     */
    private KafkaDebeziumSinkRecord createRecordWithStructKey(String tableName) {
        String uniqueId = UUID.randomUUID().toString();

        // Key schema and value
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Struct keyValue = new Struct(keySchema)
                .put("id", uniqueId);

        // Value schema and data
        Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("value", Schema.INT32_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema)
                .put("id", uniqueId)
                .put("name", "Test " + tableName)
                .put("value", 42);

        // Create the sink record
        SinkRecord record = new SinkRecord(
                tableName, // Topic
                0, // Partition
                keySchema,
                keyValue,
                valueSchema,
                value,
                0L // Offset
        );

        return new KafkaDebeziumSinkRecord(record);
    }

    /**
     * Retrieves all user-defined tables from the PostgreSQL database.
     *
     * @param conn an active database connection
     * @return a list of fully qualified table names (schema.table)
     * @throws SQLException if the database operation fails
     */
    private List<String> getAllTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT table_schema || '.' || table_name FROM information_schema.tables " +
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    /**
     * Finds a table in the provided list that contains all the specified parts in its name.
     *
     * @param tables list of table names to search through
     * @param parts string parts that must be present in the table name
     * @return the first matching table name, or null if none found
     */
    private String findMatchingTable(List<String> tables, String... parts) {
        if (tables == null || tables.isEmpty() || parts == null || parts.length == 0) {
            return null;
        }

        for (String table : tables) {
            String tableLower = table.toLowerCase();
            boolean matches = true;

            for (String part : parts) {
                if (part != null && !tableLower.contains(part.toLowerCase())) {
                    matches = false;
                    break;
                }
            }

            if (matches) {
                return table;
            }
        }

        return null;
    }

    /**
     * Verifies that a record with the given ID exists in the specified table and has expected values.
     *
     * @param conn database connection
     * @param tableName the table to query
     * @param recordId the ID to look up
     * @param expectedName expected value for the name field
     * @param expectedValue expected value for the value field
     * @throws SQLException if the database operation fails
     */
    private void assertRecordExists(Connection conn, String tableName, String recordId,
                                    String expectedName, int expectedValue)
            throws SQLException {
        String sql = "SELECT * FROM " + tableName + " WHERE id = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, recordId);

            try (ResultSet rs = ps.executeQuery()) {
                boolean recordExists = rs.next();

                assertThat(recordExists)
                        .as(String.format("Record with ID %s should exist in table %s", recordId, tableName))
                        .isTrue();

                if (recordExists) {
                    assertThat(rs.getString("name"))
                            .as("Record name should match expected value")
                            .isEqualTo(expectedName);

                    assertThat(rs.getInt("value"))
                            .as("Record numeric value should match expected value")
                            .isEqualTo(expectedValue);
                }
            }
        }
    }

}
