/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.naming.CustomCollectionNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * Tests for the {@link CustomCollectionNamingStrategy} class.
 * Validates behavior for default naming and various custom naming styles.
 *
 * @author Gustavo Lira
 */
@Tag("UnitTests")
public class CustomCollectionNamingStrategyTest {

    private static final DebeziumSinkRecordFactory RECORD_FACTORY = new DebeziumSinkRecordFactory();
    private static final String TOPIC_NAME = "database.schema.table";
    private static final String TABLE_PART = "table";

    private Map<String, String> defaultConfig;

    @BeforeEach
    public void setup() {
        defaultConfig = new HashMap<>();
    }

    @Test
    public void testDefaultBehaviorExtractsTableNamePart() {
        // Default behavior now extracts only the table part
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(defaultConfig);
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals(TABLE_PART,
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testSnakeCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.SNAKE_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals(TABLE_PART, // snake_case doesn't change "table" since it has no separators
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testCamelCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.CAMEL_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals(TABLE_PART, // camelCase doesn't change "table" since it has no separators
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testUpperCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.UPPER_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals("TABLE", // Uppercase transforms "table" to "TABLE"
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testLowerCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.LOWER_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME.toUpperCase());

        assertEquals(TABLE_PART.toLowerCase(), // Lowercase transforms "TABLE" to "table"
                strategy.resolveCollectionName(record, TOPIC_NAME.toUpperCase()));
    }

    @Test
    public void testPrefixAndSuffixWithDefaultStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of(
                "collection.naming.prefix", "pre_",
                "collection.naming.suffix", "_suf"));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals("pre_table_suf", // Prefix and suffix are applied to the extracted table name
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testCombinationOfSnakeCaseWithPrefixAndSuffix() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of(
                "collection.naming.style", NamingStyle.SNAKE_CASE.getValue(),
                "collection.naming.prefix", "pre_",
                "collection.naming.suffix", "_suf"));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord(TOPIC_NAME);

        assertEquals("pre_table_suf", // Prefix, snake_case, and suffix are applied
                strategy.resolveCollectionName(record, TOPIC_NAME));
    }

    @Test
    public void testMultiPartTableNameWithSnakeCase() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.SNAKE_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.user_profile_data");

        assertEquals("user_profile_data", // Multi-part name retains snake_case
                strategy.resolveCollectionName(record, "database.schema.user_profile_data"));
    }

    @Test
    public void testCamelCaseForMultiPartTableName() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.CAMEL_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.user_profile_data");

        assertEquals("userProfileData", // Transforms snake_case table name to camelCase
                strategy.resolveCollectionName(record, "database.schema.user_profile_data"));
    }

    @Test
    public void testInvalidNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        assertThrows(DebeziumException.class, () -> strategy.configure(Map.of("collection.naming.style", "invalidStyle")));
    }
}