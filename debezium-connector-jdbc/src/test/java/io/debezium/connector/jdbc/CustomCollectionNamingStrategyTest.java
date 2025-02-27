/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

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

    @Test
    public void testDefaultBehaviorPreservesOriginalName() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of()); // Default configuration
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("database.schema.table",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testSnakeCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.SNAKE_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("database_schema_table",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testCamelCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.CAMEL_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("databaseSchemaTable",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testUpperCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.UPPER_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("DATABASE.SCHEMA.TABLE",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testLowerCaseNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of("collection.naming.style", NamingStyle.LOWER_CASE.getValue()));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("DATABASE.SCHEMA.TABLE");

        assertEquals("database.schema.table",
                strategy.resolveCollectionName(record, "DATABASE.SCHEMA.TABLE"));
    }

    @Test
    public void testPrefixAndSuffixWithDefaultStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of(
                "collection.naming.prefix", "pre_",
                "collection.naming.suffix", "_suf"));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("pre_database.schema.table_suf",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testCombinationOfSnakeCaseWithPrefixAndSuffix() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        strategy.configure(Map.of(
                "collection.naming.style", NamingStyle.SNAKE_CASE.getValue(),
                "collection.naming.prefix", "pre_",
                "collection.naming.suffix", "_suf"));
        DebeziumSinkRecord record = RECORD_FACTORY.createRecord("database.schema.table");

        assertEquals("pre_database_schema_table_suf",
                strategy.resolveCollectionName(record, "database.schema.table"));
    }

    @Test
    public void testInvalidNamingStyle() {
        CustomCollectionNamingStrategy strategy = new CustomCollectionNamingStrategy();
        assertThrows(DebeziumException.class, () -> strategy.configure(Map.of("collection.naming.style", "invalidStyle")));
    }
}
