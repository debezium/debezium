/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link NamingStyleUtils} class.
 *
 * @author Gustavo Lira
 */
class NamingStyleUtilsTest {

    @Test
    void testSnakeCase() {
        assertEquals("camel_case_name", NamingStyleUtils.applyNamingStyle("camelCaseName", NamingStyle.SNAKE_CASE));
        assertEquals("camel_case_name", NamingStyleUtils.applyNamingStyle("CamelCaseName", NamingStyle.SNAKE_CASE));
        assertEquals("name_with_numbers_123", NamingStyleUtils.applyNamingStyle("NameWithNumbers123", NamingStyle.SNAKE_CASE));
        assertEquals("snake_case_1", NamingStyleUtils.applyNamingStyle("SnakeCase1", NamingStyle.SNAKE_CASE));
        assertEquals("123_numbers", NamingStyleUtils.applyNamingStyle("123Numbers", NamingStyle.SNAKE_CASE));
        assertEquals("123_numbers_example", NamingStyleUtils.applyNamingStyle("123NumbersExample", NamingStyle.SNAKE_CASE));
    }

    @Test
    @FixFor("debezium/dbz#3194")
    void testSnakeCaseWithSingleDigitBetweenLetters() {
        assertEquals("line_1_address", NamingStyleUtils.applyNamingStyle("line1Address", NamingStyle.SNAKE_CASE));
        assertEquals("address_2_city", NamingStyleUtils.applyNamingStyle("address2City", NamingStyle.SNAKE_CASE));
        assertEquals("v_1_b", NamingStyleUtils.applyNamingStyle("v1b", NamingStyle.SNAKE_CASE));
        assertEquals("1_a_b", NamingStyleUtils.applyNamingStyle("1aB", NamingStyle.SNAKE_CASE));
        assertEquals("a_b_1", NamingStyleUtils.applyNamingStyle("aB1", NamingStyle.SNAKE_CASE));
        assertEquals("sha_256_hash", NamingStyleUtils.applyNamingStyle("sha256Hash", NamingStyle.SNAKE_CASE));
        assertEquals("public_line_1_address", NamingStyleUtils.applyNamingStyle("public.line1Address", NamingStyle.SNAKE_CASE));
    }

    @Test
    void testCamelCase() {
        assertEquals("camelCaseName", NamingStyleUtils.applyNamingStyle("camel_case_name", NamingStyle.CAMEL_CASE));
        assertEquals("nameWithNumbers123", NamingStyleUtils.applyNamingStyle("name_with_numbers_123", NamingStyle.CAMEL_CASE));
        assertEquals("inventoryCustomerOrders", NamingStyleUtils.applyNamingStyle("inventory.customerOrders", NamingStyle.CAMEL_CASE));
        assertEquals("firstName", NamingStyleUtils.applyNamingStyle("firstName", NamingStyle.CAMEL_CASE));
    }

    @Test
    void testUpperCase() {
        assertEquals("CAMELCASENAME", NamingStyleUtils.applyNamingStyle("camelCaseName", NamingStyle.UPPER_CASE));
        assertEquals("NAME_WITH_NUMBERS_123", NamingStyleUtils.applyNamingStyle("name_with_numbers_123", NamingStyle.UPPER_CASE));
    }

    @Test
    void testLowerCase() {
        assertEquals("camelcasename", NamingStyleUtils.applyNamingStyle("camelCaseName", NamingStyle.LOWER_CASE));
        assertEquals("name_with_numbers_123", NamingStyleUtils.applyNamingStyle("NAME_WITH_NUMBERS_123", NamingStyle.LOWER_CASE));
    }

    @Test
    void testDefault() {
        assertEquals("camelCaseName", NamingStyleUtils.applyNamingStyle("camelCaseName", NamingStyle.DEFAULT));
        assertEquals("name_with_numbers_123", NamingStyleUtils.applyNamingStyle("name_with_numbers_123", NamingStyle.DEFAULT));
    }

    @Test
    void testNullInputs() {
        DebeziumException nameException = assertThrows(DebeziumException.class,
                () -> NamingStyleUtils.applyNamingStyle(null, NamingStyle.DEFAULT));
        assertEquals("Name and style must not be null", nameException.getMessage());

        DebeziumException styleException = assertThrows(DebeziumException.class,
                () -> NamingStyleUtils.applyNamingStyle("someName", null));
        assertEquals("Name and style must not be null", styleException.getMessage());
    }
}
