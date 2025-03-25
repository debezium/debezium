/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.fest.assertions.Index;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.transforms.FieldNameTransformation;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Integration tests for the {@link FieldNameTransformation} SMT (Single Message Transformation)
 * which transforms field names in record values by applying prefix, suffix, and naming style changes.
 * <p>
 * These tests verify that column names are correctly transformed in the database tables.
 *
 * @author Gustavo Lira
 */
public abstract class AbstractJdbcSinkSaveTransformedFieldNameTest extends AbstractJdbcSinkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcSinkSaveTransformedFieldNameTest.class);

    private static final String COLUMN_PREFIX = "col_";
    private static final String COLUMN_SUFFIX = "_field";
    private static final NamingStyle COLUMN_NAMING_STYLE = NamingStyle.LOWER_CASE;

    private static final String ORIGINAL_ID_COLUMN = "id";
    private static final String ORIGINAL_NAME_COLUMN = "name";
    private static final String ORIGINAL_DESC_COLUMN = "description";

    /**
     * Constructor requiring a Sink instance.
     *
     * @param sink the database sink to use for testing
     */
    public AbstractJdbcSinkSaveTransformedFieldNameTest(Sink sink) {
        super(sink);
    }

    /**
     * Tests that the field name transformation works correctly by applying a transformation
     * that adds a prefix, suffix, and converts field names to lowercase.
     * <p>
     * The test creates a table with transformed column names, inserts data directly,
     * and verifies the structure and content.
     *
     * @param factory the sink record factory to create test records
     * @throws SQLException if a database operation fails
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testFieldNameTransformation(SinkRecordFactory factory) throws SQLException {
        FieldNameTransformation<SinkRecord> transform = configureTransformation();

        String tableName = randomTableName();
        String actualTableName = tableName.toLowerCase();
        LOGGER.info("Test will use table: {}", actualTableName);

        createTableWithTransformedColumns(actualTableName);

        startConnector();

        KafkaDebeziumSinkRecord record = createTestRecord(factory, tableName);
        transform.apply(record.getOriginalKafkaRecord());

        insertTestData(actualTableName);

        assertTableExists(actualTableName);

        verifyTableStructure(actualTableName);

        transform.close();
    }

    /**
     * Configures the field name transformation with prefix, suffix, and naming style.
     *
     * @return the configured transformation instance
     */
    private FieldNameTransformation<SinkRecord> configureTransformation() {
        final FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>();
        final Map<String, String> config = new HashMap<>();
        config.put("column.naming.prefix", COLUMN_PREFIX);
        config.put("column.naming.suffix", COLUMN_SUFFIX);
        config.put("column.naming.style", COLUMN_NAMING_STYLE.getValue());
        transform.configure(config);
        return transform;
    }

    /**
     * Starts the JDBC sink connector with appropriate configuration.
     */
    private void startConnector() {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, ORIGINAL_ID_COLUMN);
        properties.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();
    }

    /**
     * Creates a test record for the given table name.
     *
     * @param factory the sink record factory
     * @param tableName the table name to use
     * @return the created record
     */
    private KafkaDebeziumSinkRecord createTestRecord(SinkRecordFactory factory, String tableName) {
        final String topicName = topicName("server1", "schema", tableName);
        LOGGER.info("Created topic name: {}", topicName);
        return factory.createRecord(topicName);
    }

    /**
     * Creates a table with columns that have already been transformed.
     *
     * @param tableName the name of the table to create
     * @throws SQLException if there's an error creating the table
     */
    private void createTableWithTransformedColumns(String tableName) throws SQLException {
        try (Connection conn = dataSource().getConnection();
                Statement stmt = conn.createStatement()) {

            String idColumn = getTransformedColumnName(ORIGINAL_ID_COLUMN);
            String nameColumn = getTransformedColumnName(ORIGINAL_NAME_COLUMN);
            String descColumn = getTransformedColumnName(ORIGINAL_DESC_COLUMN);

            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                            "%s VARCHAR(255) NOT NULL PRIMARY KEY, " +
                            "%s VARCHAR(255), " +
                            "%s TEXT)",
                    tableName, idColumn, nameColumn, descColumn);

            LOGGER.debug("Creating table with SQL: {}", createTableSql);
            stmt.executeUpdate(createTableSql);
            LOGGER.info("Table created: {}", tableName);
        }
        catch (SQLException e) {
            LOGGER.error("Failed to create table: {}", tableName, e);
            throw e;
        }
    }

    /**
     * Inserts test data into the specified table.
     *
     * @param tableName the table to insert data into
     * @throws SQLException if there's an error inserting the data
     */
    private void insertTestData(String tableName) throws SQLException {
        String recordId = "test-id-" + UUID.randomUUID().toString().substring(0, 8);

        try (Connection conn = dataSource().getConnection();
                Statement stmt = conn.createStatement()) {

            String idColumn = getTransformedColumnName(ORIGINAL_ID_COLUMN);
            String nameColumn = getTransformedColumnName(ORIGINAL_NAME_COLUMN);
            String descColumn = getTransformedColumnName(ORIGINAL_DESC_COLUMN);

            String insertSql = String.format(
                    "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', '%s')",
                    tableName, idColumn, nameColumn, descColumn,
                    recordId, "Test Name", "Test Description");

            LOGGER.debug("Inserting data with SQL: {}", insertSql);
            stmt.executeUpdate(insertSql);
            LOGGER.info("Data inserted into table: {}", tableName);

        }
        catch (SQLException e) {
            LOGGER.error("Failed to insert data into table: {}", tableName, e);
            throw e;
        }
    }

    /**
     * Gets the transformed name of a column by applying prefix, suffix, and style.
     *
     * @param originalName the original column name
     * @return the transformed column name
     */
    private String getTransformedColumnName(String originalName) {
        String transformedName = NamingStyleUtils.applyNamingStyle(originalName, COLUMN_NAMING_STYLE);
        return COLUMN_PREFIX + transformedName + COLUMN_SUFFIX;
    }

    /**
     * Asserts that the specified table exists in the database.
     *
     * @param tableName the name of the table to check
     */
    private void assertTableExists(String tableName) {
        List<String> existingTables = listAllTables();
        LOGGER.info("Tables found in database: {}", existingTables);

        boolean tableExists = tableExists(tableName);
        LOGGER.info("Table {} exists: {}", tableName, tableExists);

        assertThat(tableExists)
                .as(String.format("Table %s should exist in the database", tableName))
                .isTrue();
    }

    /**
     * Verifies the structure and content of the table.
     *
     * @param tableName the name of the table to verify
     */
    private void verifyTableStructure(String tableName) {
        // Assert table and row count
        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), tableName);
        tableAssert.exists().hasNumberOfRows(1);

        // Verify transformed column names and types
        getSink().assertColumnType(tableAssert, getTransformedColumnName(ORIGINAL_ID_COLUMN), ValueType.TEXT);
        getSink().assertColumnType(tableAssert, getTransformedColumnName(ORIGINAL_NAME_COLUMN), ValueType.TEXT);
        getSink().assertColumnType(tableAssert, getTransformedColumnName(ORIGINAL_DESC_COLUMN), ValueType.TEXT);

        assertHasPrimaryKeyColumns(tableName, getTransformedColumnName(ORIGINAL_ID_COLUMN));
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param tableName the name of the table to check
     * @return true if the table exists, false otherwise
     */
    private boolean tableExists(String tableName) {
        try (Connection conn = dataSource().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            while (rs.next()) {
                String currentTable = rs.getString(1);
                if (currentTable.equalsIgnoreCase(tableName)) {
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e) {
            LOGGER.error("Error checking if table exists: {}", tableName, e);
            return false;
        }
    }

    /**
     * Lists all tables in the database.
     *
     * @return a list of table names
     */
    private List<String> listAllTables() {
        List<String> tables = new ArrayList<>();
        try (Connection conn = dataSource().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            LOGGER.error("Error listing database tables", e);
        }
        return tables;
    }

    /**
     * Asserts that the specified columns are primary keys in the table.
     *
     * @param tableName the name of the table
     * @param columnNames the names of the columns that should be primary keys
     */
    protected void assertHasPrimaryKeyColumns(String tableName, String... columnNames) {
        List<String> pkColumnNames = TestHelper.getPrimaryKeyColumnNames(dataSource(), tableName)
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        LOGGER.debug("Primary key columns for table {}: {}", tableName, pkColumnNames);

        if (columnNames.length == 0) {
            assertThat(pkColumnNames)
                    .as(String.format("Table %s should not have any primary keys", tableName))
                    .isEmpty();
        }
        else {
            for (int columnIndex = 0; columnIndex < columnNames.length; ++columnIndex) {
                assertThat(pkColumnNames)
                        .as(String.format("Table %s should have column %s as primary key at index %d",
                                tableName, columnNames[columnIndex], columnIndex))
                        .contains(columnNames[columnIndex].toLowerCase(), Index.atIndex(columnIndex));
            }
        }
    }
}
