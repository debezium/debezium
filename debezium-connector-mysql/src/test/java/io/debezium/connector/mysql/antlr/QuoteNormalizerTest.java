/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QuoteNormalizer}.
 */
public class QuoteNormalizerTest {

    @Test
    public void testBasicEnumTransformation() {
        String input = "CREATE TABLE t (col ENUM(\"a\", \"b\", \"c\"))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testBasicSetTransformation() {
        String input = "CREATE TABLE t (col SET(\"x\", \"y\", \"z\"))";
        String expected = "CREATE TABLE t (col SET('x', 'y', 'z'))";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testBasicDefaultTransformation() {
        String input = "CREATE TABLE t (col VARCHAR(10) DEFAULT \"value\")";
        String expected = "CREATE TABLE t (col VARCHAR(10) DEFAULT 'value')";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testDefaultNumericString() {
        String input = "CREATE TABLE t (col DECIMAL(26,6) DEFAULT \"1\")";
        String expected = "CREATE TABLE t (col DECIMAL(26,6) DEFAULT '1')";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testMixedQuotesInEnum() {
        String input = "CREATE TABLE t (col ENUM(\"a\", 'b', \"c\"))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testEscapedDoubleQuoteInEnum() {
        String input = "CREATE TABLE t (col ENUM(\"a\\\"\", 'b', 'c'))";
        String expected = "CREATE TABLE t (col ENUM('a\\\"', 'b', 'c'))";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testConcatInStoredProcedure() {
        String input = "CREATE PROCEDURE p() BEGIN SELECT CONCAT(\"a\", \"b\"); END";
        String expected = "CREATE PROCEDURE p() BEGIN SELECT CONCAT('a', 'b'); END";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testComplexStoredProcedureWithStringLiterals() {
        String input = "CREATE PROCEDURE p() BEGIN " +
                "SELECT CONCAT(\"THE SERVER \", \"WAS RESTARTED\"); " +
                "END";
        String expected = "CREATE PROCEDURE p() BEGIN " +
                "SELECT CONCAT('THE SERVER ', 'WAS RESTARTED'); " +
                "END";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testComplexDdlWithMultipleElements() {
        String input = "CREATE TABLE t (" +
                "col1 ENUM(\"a\", \"b\"), " +
                "col2 VARCHAR(10) DEFAULT \"test\", " +
                "col3 SET(\"x\", \"y\")" +
                ")";
        String expected = "CREATE TABLE t (" +
                "col1 ENUM('a', 'b'), " +
                "col2 VARCHAR(10) DEFAULT 'test', " +
                "col3 SET('x', 'y')" +
                ")";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testAlreadyNormalizedDdl() {
        String input = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    public void testNullInput() {
        assertThat(QuoteNormalizer.normalize(null)).isNull();
    }

    @Test
    public void testEmptyInput() {
        assertThat(QuoteNormalizer.normalize("")).isEmpty();
    }

    @Test
    public void testNoDoubleQuotes() {
        String input = "CREATE TABLE t (col INT)";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(input);
    }

    @Test
    public void testDoubleQuotesInDifferentContexts() {
        String input = "CREATE TABLE t (" +
                "col1 ENUM(\"a\", \"b\"), " +
                "col2 INT DEFAULT \"123\", " +
                "col3 VARCHAR(10) DEFAULT \"text\"" +
                ")";
        String expected = "CREATE TABLE t (" +
                "col1 ENUM('a', 'b'), " +
                "col2 INT DEFAULT '123', " +
                "col3 VARCHAR(10) DEFAULT 'text'" +
                ")";
        assertThat(QuoteNormalizer.normalize(input)).isEqualTo(expected);
    }
}
