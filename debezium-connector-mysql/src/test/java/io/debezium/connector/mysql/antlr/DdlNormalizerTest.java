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

    @DisplayName("Given ANSI_QUOTES mode and double-quoted identifiers When normalize Then identifiers are preserved")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testAnsiQuotesModePreservesDoubleQuotedIdentifiers() {
        String input = "CREATE TABLE \"customers\" (\"id\" int NOT NULL AUTO_INCREMENT, PRIMARY KEY (\"id\"))";
        assertThat(DdlNormalizer.normalize(input, true)).isEqualTo(input);
    }

    @DisplayName("Given ANSI_QUOTES mode and full CREATE TABLE with double-quoted identifiers When normalize Then identifiers are preserved")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testAnsiQuotesModeFullCreateTable() {
        String input = "CREATE TABLE \"addresses\" (\n" +
                "  \"id\" int NOT NULL AUTO_INCREMENT,\n" +
                "  \"customer_id\" int NOT NULL,\n" +
                "  \"street\" varchar(255) NOT NULL,\n" +
                "  \"city\" varchar(255) NOT NULL,\n" +
                "  \"type\" enum('SHIPPING','BILLING','LIVING') NOT NULL,\n" +
                "  PRIMARY KEY (\"id\"),\n" +
                "  KEY \"address_customer\" (\"customer_id\"),\n" +
                "  CONSTRAINT \"addresses_ibfk_1\" FOREIGN KEY (\"customer_id\") REFERENCES \"customers\" (\"id\")\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        assertThat(DdlNormalizer.normalize(input, true)).isEqualTo(input);
    }

    @DisplayName("Given ANSI_QUOTES mode and ALTER TABLE with double-quoted identifiers When normalize Then identifiers are preserved")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testAnsiQuotesModeAlterTable() {
        String input = "ALTER TABLE inventory.\"customers\" ADD COLUMN \"middle_name\" varchar(255) NULL";
        assertThat(DdlNormalizer.normalize(input, true)).isEqualTo(input);
    }

    @DisplayName("Given ANSI_QUOTES mode with single-quoted strings When normalize Then strings are preserved")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testAnsiQuotesModeSingleQuotedStrings() {
        String input = "CREATE TABLE \"t\" (\"col\" VARCHAR(10) DEFAULT 'hello')";
        assertThat(DdlNormalizer.normalize(input, true)).isEqualTo(input);
    }

    @DisplayName("Given default mode (not ANSI_QUOTES) When normalize Then double-quoted strings are still converted")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testDefaultModeStillConvertsDoubleQuotes() {
        String input = "CREATE TABLE t (col ENUM(\"a\", \"b\", \"c\"))";
        String expected = "CREATE TABLE t (col ENUM('a', 'b', 'c'))";
        assertThat(DdlNormalizer.normalize(input, false)).isEqualTo(expected);
    }

    @DisplayName("Given ANSI_QUOTES mode with reserved keywords When normalize Then keywords still get backticks")
    @Test
    @FixFor("debezium/dbz#2291")
    public void testAnsiQuotesModeReservedKeywordsStillBackticked() {
        String input = "CREATE TABLE RANK (\"id\" INT, \"name\" VARCHAR(50))";
        String expected = "CREATE TABLE `RANK` (\"id\" INT, \"name\" VARCHAR(50))";
        assertThat(DdlNormalizer.normalize(input, true)).isEqualTo(expected);
    }

    @DisplayName("Given version-gated keyword as table and column name When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testGatedKeywordAsTableAndColumnName() {
        for (String keyword : new String[]{ "url", "auto", "manual", "offline", "online", "parallel", "vector", "qualify", "tablesample" }) {
            String input = "CREATE TABLE " + keyword + " (" + keyword + " INT)";
            String expected = "CREATE TABLE `" + keyword + "` (`" + keyword + "` INT)";
            assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
        }
    }

    @DisplayName("Given keyword table name in ALTER TABLE When normalize Then add backticks to table name only")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testUrlAsAlterTableTarget() {
        String input = "ALTER TABLE URL ADD account_id BIGINT";
        String expected = "ALTER TABLE `URL` ADD account_id BIGINT";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword column in CHANGE clause When normalize Then add backticks to old and new name")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordInChangeColumn() {
        String input = "ALTER TABLE URL CHANGE url url VARCHAR(700)";
        String expected = "ALTER TABLE `URL` CHANGE `url` `url` VARCHAR(700)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword column in ADD COLUMN with TINYINT When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordInAddColumn() {
        String input = "ALTER TABLE vector ADD COLUMN online TINYINT";
        String expected = "ALTER TABLE `vector` ADD COLUMN `online` TINYINT";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword columns in RENAME COLUMN When normalize Then add backticks to both sides")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordInRenameColumn() {
        String input = "ALTER TABLE auto RENAME COLUMN auto TO manual";
        String expected = "ALTER TABLE `auto` RENAME COLUMN `auto` TO `manual`";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword table name in RENAME TABLE When normalize Then add backticks to source and target")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordInRenameTable() {
        String input = "RENAME TABLE url TO archive, t1 TO parallel";
        String expected = "RENAME TABLE `url` TO archive, t1 TO `parallel`";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword index names When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordAsIndexName() {
        String input = "ALTER TABLE t1 ADD INDEX parallel (id), ADD UNIQUE KEY qualify (id)";
        String expected = "ALTER TABLE t1 ADD INDEX `parallel` (id), ADD UNIQUE KEY `qualify` (id)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword constraint name When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordAsConstraintName() {
        String input = "ALTER TABLE t1 ADD CONSTRAINT online CHECK (id > 0)";
        String expected = "ALTER TABLE t1 ADD CONSTRAINT `online` CHECK (id > 0)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword column with VECTOR type When normalize Then only the column name gets backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordColumnWithVectorType() {
        String input = "CREATE TABLE t1 (vector VECTOR(3))";
        String expected = "CREATE TABLE t1 (`vector` VECTOR(3))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given VECTOR used as a data type When normalize Then do not add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testVectorAsDataTypeNotBackticked() {
        String input = "CREATE TABLE t1 (embedding VECTOR(3))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given AUTO_INCREMENT attribute When normalize Then AUTO is not backticked")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testAutoIncrementNotBackticked() {
        String input = "CREATE TABLE t1 (id INT AUTO_INCREMENT, PRIMARY KEY (id))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given url inside string literals When normalize Then literal content is preserved")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testUrlInsideStringLiteralsPreserved() {
        String input = "CREATE TABLE t1 (c VARCHAR(500) DEFAULT 'http://url:8080/path' COMMENT 'copied from url')";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given QUALIFY used as a query clause When normalize Then do not add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testQualifyClauseNotBackticked() {
        String input = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM t1 QUALIFY rn = 1";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(input);
    }

    @DisplayName("Given IF [NOT] EXISTS and TEMPORARY variants When normalize Then table name still gets backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testIfExistsAndTemporaryVariants() {
        assertThat(DdlNormalizer.normalize("CREATE TABLE IF NOT EXISTS url (id INT)"))
                .isEqualTo("CREATE TABLE IF NOT EXISTS `url` (id INT)");
        assertThat(DdlNormalizer.normalize("DROP TABLE IF EXISTS url"))
                .isEqualTo("DROP TABLE IF EXISTS `url`");
        assertThat(DdlNormalizer.normalize("CREATE TEMPORARY TABLE auto (id INT)"))
                .isEqualTo("CREATE TEMPORARY TABLE `auto` (id INT)");
    }

    @DisplayName("Given TRUNCATE with keyword table name When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testTruncateKeywordTable() {
        assertThat(DdlNormalizer.normalize("TRUNCATE TABLE url")).isEqualTo("TRUNCATE TABLE `url`");
        assertThat(DdlNormalizer.normalize("TRUNCATE url")).isEqualTo("TRUNCATE `url`");
    }

    @DisplayName("Given CREATE INDEX ON keyword table When normalize Then backtick index name and table name")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testCreateIndexOnKeywordTable() {
        String input = "CREATE UNIQUE INDEX parallel ON url (id)";
        String expected = "CREATE UNIQUE INDEX `parallel` ON `url` (id)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given foreign key referencing keyword table When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testReferencesKeywordTable() {
        String input = "ALTER TABLE t1 ADD FOREIGN KEY (x) REFERENCES url (id) ON DELETE CASCADE";
        String expected = "ALTER TABLE t1 ADD FOREIGN KEY (x) REFERENCES `url` (id) ON DELETE CASCADE";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given CREATE TABLE LIKE keyword table When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testCreateTableLikeKeywordTable() {
        String input = "CREATE TABLE t2 LIKE url";
        String expected = "CREATE TABLE t2 LIKE `url`";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given ALTER [COLUMN] and AFTER with keyword column When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testAlterColumnAndAfterKeywordColumn() {
        assertThat(DdlNormalizer.normalize("ALTER TABLE t1 ALTER COLUMN online SET DEFAULT 1"))
                .isEqualTo("ALTER TABLE t1 ALTER COLUMN `online` SET DEFAULT 1");
        assertThat(DdlNormalizer.normalize("ALTER TABLE t1 ALTER online DROP DEFAULT"))
                .isEqualTo("ALTER TABLE t1 ALTER `online` DROP DEFAULT");
        assertThat(DdlNormalizer.normalize("ALTER TABLE t1 ADD COLUMN c INT AFTER url"))
                .isEqualTo("ALTER TABLE t1 ADD COLUMN c INT AFTER `url`");
    }

    @DisplayName("Given keyword partition names When normalize Then add backticks but not to PARTITION BY")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordPartitionNames() {
        String input = "CREATE TABLE pt (id INT) PARTITION BY RANGE (id) (PARTITION auto VALUES LESS THAN (10))";
        String expected = "CREATE TABLE pt (id INT) PARTITION BY RANGE (id) (PARTITION `auto` VALUES LESS THAN (10))";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
        assertThat(DdlNormalizer.normalize("ALTER TABLE pt DROP PARTITION auto"))
                .isEqualTo("ALTER TABLE pt DROP PARTITION `auto`");
    }

    @DisplayName("Given keyword database names When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordDatabaseNames() {
        assertThat(DdlNormalizer.normalize("CREATE DATABASE IF NOT EXISTS url"))
                .isEqualTo("CREATE DATABASE IF NOT EXISTS `url`");
        assertThat(DdlNormalizer.normalize("USE url")).isEqualTo("USE `url`");
        assertThat(DdlNormalizer.normalize("DROP DATABASE url")).isEqualTo("DROP DATABASE `url`");
    }

    @DisplayName("Given keyword names for other schema objects When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordObjectNames() {
        assertThat(DdlNormalizer.normalize("CREATE OR REPLACE VIEW auto AS SELECT 1"))
                .isEqualTo("CREATE OR REPLACE VIEW `auto` AS SELECT 1");
        assertThat(DdlNormalizer.normalize("DROP TRIGGER IF EXISTS online"))
                .isEqualTo("DROP TRIGGER IF EXISTS `online`");
        assertThat(DdlNormalizer.normalize("CREATE PROCEDURE parallel() SELECT 1"))
                .isEqualTo("CREATE PROCEDURE `parallel`() SELECT 1");
        assertThat(DdlNormalizer.normalize("DROP FUNCTION manual")).isEqualTo("DROP FUNCTION `manual`");
        assertThat(DdlNormalizer.normalize("DROP EVENT offline")).isEqualTo("DROP EVENT `offline`");
    }

    @DisplayName("Given DROP/ALTER CHECK with keyword constraint name When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testDropAlterCheckKeywordName() {
        assertThat(DdlNormalizer.normalize("ALTER TABLE ck ALTER CHECK online NOT ENFORCED"))
                .isEqualTo("ALTER TABLE ck ALTER CHECK `online` NOT ENFORCED");
        assertThat(DdlNormalizer.normalize("ALTER TABLE ck DROP CHECK online"))
                .isEqualTo("ALTER TABLE ck DROP CHECK `online`");
    }

    @DisplayName("Given keyword columns with extended data types When normalize Then add backticks")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testKeywordColumnsWithExtendedTypes() {
        String input = "CREATE TABLE ty (url GEOMETRY, auto CHARACTER(5), online SERIAL, manual DEC(5,2), offline POINT, parallel LONG)";
        String expected = "CREATE TABLE ty (`url` GEOMETRY, `auto` CHARACTER(5), `online` SERIAL, `manual` DEC(5,2), `offline` POINT, `parallel` LONG)";
        assertThat(DdlNormalizer.normalize(input)).isEqualTo(expected);
    }

    @DisplayName("Given keyword-like anchors in legitimate keyword usage When normalize Then only identifiers are rewritten")
    @Test
    @FixFor("debezium/dbz#2381")
    public void testLegitimateKeywordUsageUnchanged() {
        // AFTER INSERT is trigger timing, not a column position; ON auto is a table position
        assertThat(DdlNormalizer.normalize("CREATE TRIGGER trg AFTER INSERT ON auto FOR EACH ROW SET @a = 1"))
                .isEqualTo("CREATE TRIGGER trg AFTER INSERT ON `auto` FOR EACH ROW SET @a = 1");
        String hint = "SELECT id FROM t1 USE INDEX (idx1) WHERE id = DATABASE()";
        assertThat(DdlNormalizer.normalize(hint)).isEqualTo(hint);
    }
}
