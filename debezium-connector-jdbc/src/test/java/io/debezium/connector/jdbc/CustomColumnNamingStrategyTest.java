/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.naming.CustomColumnNamingStrategy;
import io.debezium.connector.jdbc.util.NamingStyle;

/**
 * Tests for the {@link CustomColumnNamingStrategy} class.
 * <p>
 * This test verifies the transformation logic based on naming styles and customizations
 * such as prefixes and suffixes.
 * </p>
 *
 * @author Gustavo Lira
 */
@Tag("UnitTests")
public class CustomColumnNamingStrategyTest {

    @Test
    public void testDefaultBehavior() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of()); // Default configuration
        assertThat(strategy.resolveColumnName("columnName")).isEqualTo("columnName");
    }

    @Test
    public void testSnakeCaseTransformation() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of("column.naming.style", NamingStyle.SNAKE_CASE.getValue()));
        assertThat(strategy.resolveColumnName("columnName")).isEqualTo("column_name");
    }

    @Test
    public void testCamelCaseTransformation() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of("column.naming.style", NamingStyle.CAMEL_CASE.getValue()));
        assertThat(strategy.resolveColumnName("column_name")).isEqualTo("columnName");
    }

    @Test
    public void testUpperCaseTransformation() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of("column.naming.style", NamingStyle.UPPER_CASE.getValue()));
        assertThat(strategy.resolveColumnName("columnName")).isEqualTo("COLUMNNAME");
    }

    @Test
    public void testLowerCaseTransformation() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of("column.naming.style", NamingStyle.LOWER_CASE.getValue()));
        assertThat(strategy.resolveColumnName("ColumnName")).isEqualTo("columnname");
    }

    @Test
    public void testPrefixAndSuffix() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of(
                "column.naming.prefix", "pre_",
                "column.naming.suffix", "_suf"));
        assertThat(strategy.resolveColumnName("columnName")).isEqualTo("pre_columnName_suf");
    }

    @Test
    public void testCombinationOfSnakeCaseWithPrefixAndSuffix() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        strategy.configure(Map.of(
                "column.naming.style", NamingStyle.SNAKE_CASE.getValue(),
                "column.naming.prefix", "pre_",
                "column.naming.suffix", "_suf"));
        assertThat(strategy.resolveColumnName("columnName")).isEqualTo("pre_column_name_suf");
    }

    @Test
    public void testInvalidNamingStyle() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
        Assertions.assertThrows(DebeziumException.class, () -> strategy.configure(Map.of("column.naming.style", "invalidStyle")));
    }
}
