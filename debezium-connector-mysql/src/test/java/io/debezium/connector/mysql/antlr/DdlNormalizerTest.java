/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DdlNormalizer}.
 */
public class DdlNormalizerTest {

    @DisplayName("Given enum with double-quoted values When normalize Then values become single-quoted")
    @Test
    public void testBasicEnumTransformation() {
        String input = "CREATE TABLE t (col ENUM(\"a\", \"b\", \"c\"))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given set with double-quoted values When normalize Then values become single-quoted")
    @Test
    public void testBasicSetTransformation() {
        String input = "CREATE TABLE t (col SET(\"x\", \"y\", \"z\"))";
        String expected = "CREATE TABLE t (col SET('x', 'y', 'z'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given default clause with double-quoted text When normalize Then default becomes single-quoted")
    @Test
    public void testBasicDefaultTransformation() {
        String input = "CREATE TABLE t (col VARCHAR(10) DEFAULT \"value\")";
        String expected = "CREATE TABLE t (col VARCHAR(10) DEFAULT 'value')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given numeric value in double quotes in default clause When normalize Then value remains text in single quotes")
    @Test
    public void testDefaultNumericString() {
        String input = "CREATE TABLE t (col DECIMAL(26,6) DEFAULT \"1\")";
        String expected = "CREATE TABLE t (col DECIMAL(26,6) DEFAULT '1')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given enum with mixed single and double quotes When normalize Then all values are single-quoted")
    @Test
    public void testMixedQuotesInEnum() {
        String input = "CREATE TABLE t (col ENUM(\"a\", 'b', \"c\"))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given enum value containing escaped double quote When normalize Then escaped content is preserved with single quotes")
    @Test
    public void testEscapedDoubleQuoteInEnum() {
        String input = "CREATE TABLE t (col ENUM(\"a\\\"\", 'b', 'c'))";
        String expected = "CREATE TABLE t (col ENUM('a\\\"', 'b', 'c'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given stored procedure concat call with double-quoted strings When normalize Then concat arguments become single-quoted")
    @Test
    public void testConcatInStoredProcedure() {
        String input = "CREATE PROCEDURE p() BEGIN SELECT CONCAT(\"a\", \"b\"); END";
        String expected = "CREATE PROCEDURE p() BEGIN SELECT CONCAT('a', 'b'); END";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given stored procedure with multiple double-quoted literals When normalize Then all literals become single-quoted")
    @Test
    public void testComplexStoredProcedureWithStringLiterals() {
        String input = "CREATE PROCEDURE p() BEGIN " +
                "SELECT CONCAT(\"THE SERVER \", \"WAS RESTARTED\"); " +
                "END";
        String expected = "CREATE PROCEDURE p() BEGIN " +
                "SELECT CONCAT('THE SERVER ', 'WAS RESTARTED'); " +
                "END";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given DDL with enum set and default using double quotes When normalize Then all string literals become single-quoted")
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
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given already normalized DDL with single quotes When normalize Then statement remains unchanged")
    @Test
    public void testAlreadyNormalizedDdl() {
        String input = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given null input When normalize Then return null")
    @Test
    public void testNullInput() {
        assertThat(DdlNormalizer.normalize(null)).isNull();
    }

    @DisplayName("Given empty input When normalize Then return empty string")
    @Test
    public void testEmptyInput() {
        assertThat(DdlNormalizer.normalize("")).isEmpty();
    }

    @DisplayName("Given statement without double quotes When normalize Then statement remains unchanged")
    @Test
    public void testNoDoubleQuotes() {
        String input = "CREATE TABLE t (col INT)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given statement with double quotes in multiple contexts When normalize Then all applicable literals become single-quoted")
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
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given reserved keyword JSON_TABLE as table name When normalize Then add backticks")
    @Test
    public void testReservedKeywordAsTableName() {
        String input = "CREATE TABLE JSON_TABLE (A JSON, B JSON NOT NULL)";
        String expected = "CREATE TABLE `JSON_TABLE` (A JSON, B JSON NOT NULL)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given reserved keyword RANK as table name When normalize Then add backticks")
    @Test
    public void testRankAsTableName() {
        String input = "CREATE TABLE RANK (id INT, name VARCHAR(50))";
        String expected = "CREATE TABLE `RANK` (id INT, name VARCHAR(50))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given reserved keyword as column name When normalize Then add backticks")
    @Test
    public void testReservedKeywordAsColumnName() {
        String input = "CREATE TABLE t (RANK INT, LAG VARCHAR(10))";
        String expected = "CREATE TABLE t (`RANK` INT, `LAG` VARCHAR(10))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given RANK used as window function When normalize Then do not add backticks")
    @Test
    public void testRankAsWindowFunction() {
        String input = "SELECT RANK() OVER (ORDER BY col) FROM t";
        String expected = "SELECT RANK() OVER (ORDER BY col) FROM t";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given reserved keyword as label in stored procedure When normalize Then add backticks")
    @Test
    public void testReservedKeywordAsLabel() {
        String input = "CREATE PROCEDURE p() BEGIN RANK: LOOP SET x=1; END LOOP RANK; END";
        String expected = "CREATE PROCEDURE p() BEGIN `RANK`: LOOP SET x=1; END LOOP RANK; END";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given multiple reserved keywords in different contexts When normalize Then only add backticks to identifiers")
    @Test
    public void testMixedReservedKeywordUsage() {
        String input = "CREATE TABLE RANK (id INT, DENSE_RANK VARCHAR(10))";
        String expected = "CREATE TABLE `RANK` (id INT, `DENSE_RANK` VARCHAR(10))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }
}
