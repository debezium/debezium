/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

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

    @DisplayName("Given comment with double quotes inside single-quoted string When normalize Then single-quoted string is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testDoubleQuotesInsideSingleQuotedString() {
        String input = "ALTER TABLE t MODIFY COLUMN col VARCHAR(200) NOT NULL COMMENT 'Other rights text for \"other\" option'";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given double quotes inside single-quoted default value When normalize Then single-quoted string is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testDoubleQuotesInsideSingleQuotedDefault() {
        String input = "CREATE TABLE t (col VARCHAR(50) DEFAULT 'say \"hello\"')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given mixed single and double-quoted strings When normalize Then single-quoted preserved and double-quoted converted")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testMixedSingleAndDoubleQuotedStrings() {
        String input = "CREATE TABLE t (col1 ENUM(\"a\", \"b\") COMMENT 'text with \"quotes\" inside')";
        String expected = "CREATE TABLE t (col1 ENUM('a', 'b') COMMENT 'text with \"quotes\" inside')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given double-dash comment containing apostrophe When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testDoubleDashCommentWithApostrophe() {
        String input = "-- we'll grant privileges\nCREATE TABLE t (col ENUM(\"a\", \"b\"))";
        String expected = "-- we'll grant privileges\nCREATE TABLE t (col ENUM('a', 'b'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given double-dash comment containing double quotes When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testDoubleDashCommentWithDoubleQuotes() {
        String input = "-- comment with \"quoted\" text\nCREATE TABLE t (col INT)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given hash comment containing apostrophe When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testHashCommentWithApostrophe() {
        String input = "# it's a comment\nCREATE TABLE t (col ENUM(\"a\"))";
        String expected = "# it's a comment\nCREATE TABLE t (col ENUM('a'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given hash comment containing double quotes When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testHashCommentWithDoubleQuotes() {
        String input = "# comment with \"quoted\" text\nCREATE TABLE t (col INT)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given block comment containing apostrophe When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testBlockCommentWithApostrophe() {
        String input = "/* it's a comment */ CREATE TABLE t (col ENUM(\"a\"))";
        String expected = "/* it's a comment */ CREATE TABLE t (col ENUM('a'))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given block comment containing double quotes When normalize Then comment is preserved")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testBlockCommentWithDoubleQuotes() {
        String input = "/* comment with \"quoted\" text */ CREATE TABLE t (col INT)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given MySQL version comment When normalize Then body is still normalized")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testVersionCommentIsNormalized() {
        String input = "/*!50003 CREATE TABLE t (col ENUM(\"a\", \"b\")) */";
        String expected = "/*!50003 CREATE TABLE t (col ENUM('a', 'b')) */";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given inline comment between DDL statements When normalize Then both statements parse correctly")
    @Test
    @FixFor("debezium/dbz#2237")
    public void testInlineCommentBetweenStatements() {
        String input = "CREATE TABLE t1 (col ENUM(\"a\")); -- don't touch\nCREATE TABLE t2 (col ENUM(\"b\"))";
        String expected = "CREATE TABLE t1 (col ENUM('a')); -- don't touch\nCREATE TABLE t2 (col ENUM('b'))";
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

    @DisplayName("Given reserved keyword pattern inside single-quoted default value When normalize Then string content is preserved")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordInsideSingleQuotedDefault() {
        String input = "CREATE TABLE t (c VARCHAR(20) DEFAULT 'FROM RANK')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given reserved keyword pattern inside single-quoted comment When normalize Then string content is preserved")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordInsideSingleQuotedComment() {
        String input = "CREATE TABLE t (c INT COMMENT 'RANK INT is a reserved word')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given reserved keyword label pattern inside single-quoted comment When normalize Then string content is preserved")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordLabelInsideSingleQuotedComment() {
        String input = "CREATE TABLE t (c INT COMMENT 'RANK: top 10')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given reserved keyword pattern inside double-quoted string When normalize Then converted content is preserved")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordInsideDoubleQuotedString() {
        String input = "CREATE TABLE t (c VARCHAR(20) DEFAULT \"FROM RANK\")";
        String expected = "CREATE TABLE t (c VARCHAR(20) DEFAULT 'FROM RANK')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given reserved keyword pattern inside SQL comment When normalize Then comment content is preserved")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordInsideSqlComment() {
        String input = "-- example: CREATE TABLE RANK ...\nCREATE TABLE t (c INT)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given reserved keyword identifier next to string literals When normalize Then only the identifier gets backticks")
    @Test
    @FixFor("debezium/dbz#2254")
    public void testReservedKeywordIdentifierNextToStrings() {
        String input = "CREATE TABLE RANK (c VARCHAR(20) DEFAULT 'FROM RANK', RANK INT COMMENT 'RANK: n')";
        String expected = "CREATE TABLE `RANK` (c VARCHAR(20) DEFAULT 'FROM RANK', `RANK` INT COMMENT 'RANK: n')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }
}
