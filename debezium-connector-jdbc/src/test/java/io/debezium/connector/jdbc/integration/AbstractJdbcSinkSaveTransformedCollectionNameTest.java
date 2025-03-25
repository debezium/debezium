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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
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
import io.debezium.connector.jdbc.naming.CustomCollectionNamingStrategy;
import io.debezium.connector.jdbc.transforms.CollectionNameTransformation;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Tests for CollectionNameTransformation.
 *
 * @author Gustavo Lira
 */
public abstract class AbstractJdbcSinkSaveTransformedCollectionNameTest extends AbstractJdbcSinkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcSinkSaveTransformedCollectionNameTest.class);

    public AbstractJdbcSinkSaveTransformedCollectionNameTest(Sink sink) {
        super(sink);
    }

    /**
     * Tests the transformation of collection names with prefix and suffix applied.
     *
     * @param factory The sink record factory used to create test records
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCollectionNameTransformation(SinkRecordFactory factory) {
        // Initialize the transformation with prefix and suffix configuration
        final CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>();
        final Map<String, String> config = new HashMap<>();
        config.put("collection.naming.prefix", "DBZ_");
        config.put("collection.naming.suffix", "_V2");
        config.put("collection.naming.style", "UPPER_CASE");

        transform.configure(config);

        // Configure and start the sink connector
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, CustomCollectionNamingStrategy.class.getName());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        // Create and process a record
        final String tableName = randomTableName();
        final KafkaDebeziumSinkRecord originalRecord = factory.createRecord(tableName);
        SinkRecord transformedSinkRecord = transform.apply(originalRecord.getOriginalKafkaRecord());
        final KafkaDebeziumSinkRecord transformedRecord = new KafkaDebeziumSinkRecord(transformedSinkRecord);

        consume(transformedRecord);

        // Determine expected table name after transformation
        final String expectedTableName = "DBZ_" + tableName.toUpperCase() + "_V2";

        // Verify the table exists with the expected name
        boolean tableExists = tableExists(expectedTableName);
        LOGGER.info("Checking if table '{}' exists: {}", expectedTableName, tableExists);

        // Verify table existence and structure
        assertThat(tableExists).isTrue();

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), expectedTableName);
        tableAssert.exists().hasNumberOfRows(1);

        assertHasPrimaryKeyColumns(expectedTableName, "id");

        transform.close();
    }

    /**
     * Tests the transformation of collection names without prefix and suffix,
     * only applying the naming style.
     *
     * @param factory The sink record factory used to create test records
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCollectionNameTransformationWithoutPrefixSuffix(SinkRecordFactory factory) {
        // Initialize transformation with only naming style (no prefix/suffix)
        final CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>();
        final Map<String, String> config = new HashMap<>();
        config.put("collection.naming.style", "UPPER_CASE");

        transform.configure(config);

        // Configure and start the sink connector
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, CustomCollectionNamingStrategy.class.getName());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        // Create and process a record
        final String tableName = randomTableName();
        final KafkaDebeziumSinkRecord originalRecord = factory.createRecord(tableName);
        SinkRecord transformedSinkRecord = transform.apply(originalRecord.getOriginalKafkaRecord());
        final KafkaDebeziumSinkRecord transformedRecord = new KafkaDebeziumSinkRecord(transformedSinkRecord);

        consume(transformedRecord);

        // Only style transformation expected (uppercase)
        final String expectedTableName = tableName.toUpperCase();

        // Verify the table exists with the expected name
        boolean tableExists = tableExists(expectedTableName);
        LOGGER.info("Checking if table '{}' exists: {}", expectedTableName, tableExists);

        // Verify table existence and structure
        assertThat(tableExists).isTrue();

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), expectedTableName);
        tableAssert.exists().hasNumberOfRows(1);

        assertHasPrimaryKeyColumns(expectedTableName, "id");

        transform.close();
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param tableName name of the table to check
     * @return true if the table exists, false otherwise
     */
    private boolean tableExists(String tableName) {
        try (Connection conn = dataSource().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            while (rs.next()) {
                String currentTableName = rs.getString(1);
                if (currentTableName.equalsIgnoreCase(tableName)) {
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e) {
            LOGGER.error("Error checking table existence", e);
            return false;
        }
    }

    /**
     * Asserts that the specified primary key columns exist on the table.
     *
     * @param tableName the name of the table to check
     * @param columnNames the expected primary key column names
     */
    protected void assertHasPrimaryKeyColumns(String tableName, String... columnNames) {
        List<String> pkColumnNames = TestHelper.getPrimaryKeyColumnNames(dataSource(), tableName)
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        LOGGER.debug("Primary key columns for table {}: {}", tableName, pkColumnNames);

        if (columnNames.length == 0) {
            assertThat(pkColumnNames).isEmpty();
        }
        else {
            for (int columnIndex = 0; columnIndex < columnNames.length; ++columnIndex) {
                assertThat(pkColumnNames).contains(columnNames[columnIndex].toLowerCase(), Index.atIndex(columnIndex));
            }
        }
    }
}